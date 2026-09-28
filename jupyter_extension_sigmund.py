"""This is an extension for Jupyterlab, Jupyter Notebook, Spyder, Rapunzel, or any other application that uses a Jupyter/ IPython based console. It allows you to connect your Python session to [SigmundAI](https://sigmundai.eu). This is mainly intended as a tool for AI-assisted coding and data analysis.

The extension starts a websocket server that the Sigmund web client (https://sigmundai.eu) connects to automatically once you open it in a browser. Once connected, Sigmund can interact with your session through commands, which are sent in the workspace content of AI messages:

- ``ide_execute_code`` executes Python code in your session. The code and its output are shown in the notebook, and the output is sent back to Sigmund, including any images (plots, etc.) that the code generated.
- ``ide_inspect_files`` returns the contents of files.
- ``ide_list_files`` returns a list of the files in a directory.

The result of a command is sent back to Sigmund as a tool-result message, which is not shown in the chat interface.
"""
import asyncio
import base64
import inspect
import json
import logging
import os
import re
import sys
import time
import traceback
from markdown import markdown
from markdown.extensions.fenced_code import FencedCodeExtension
from markdown.extensions.codehilite import CodeHiliteExtension
from contextlib import contextmanager
from io import StringIO
from threading import Thread

import websockets
from IPython.core.magic import (
    Magics,
    line_cell_magic,
    line_magic,
    magics_class,
)
from IPython.core.magic_arguments import (
    argument,
    magic_arguments,
    parse_argstring,
)
from IPython.display import HTML, Markdown, display


logger = logging.getLogger(__name__)
__version__ = '0.4.1'


def enable_logging(level=logging.INFO):
    """Enable logging output for this extension."""
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.info(
        f'SigmundAI extension logging enabled at level '
        f'{logging.getLevelName(level)}'
    )


def _trunc(text, length=60):
    """Return a single-line, truncated preview of text, for use in log
    messages.

    This prevents long texts (such as code or file contents) and newlines
    from flooding the log.
    """
    if not isinstance(text, str):
        text = repr(text)
    text = ' '.join(text.split())
    if len(text) <= length:
        return text
    return text[:length - 3] + '...'


# The prefix that marks a user message as a tool result. Tool results are not
# shown in the chat interface of the Sigmund web client, but they are sent to
# the Sigmund server, where they are merged with the tool call that triggered
# them.
TOOL_RESULT_PREFIX = '::tool_result::'
# Transient settings that enable the tools that Sigmund can use to interact
# with this session. This needs to be a JSON string, because the Sigmund web
# client passes the value verbatim to the server as part of a form submission.
TRANSIENT_SETTINGS = json.dumps({
    'tool_ide_execute_code': 'true',
    'tool_ide_inspect_files': 'true',
    'tool_ide_list_files': 'true',
    'tool_update_workspace_content': 'false',
})
# A system prompt that is sent along with every user message.
TRANSIENT_SYSTEM_PROMPT = 'You are helping the user work in a Jupyter Notebook.'
# The keys under which the arguments of a command may be wrapped.
ARG_WRAPPER_KEYS = [
    'args', 'arguments', 'kwargs', 'keyword_arguments', 'keywords',
    'parameters', 'params', 'options'
]
# Matches ansi escape sequences, which IPython may insert.
ANSI_PATTERN = re.compile(r'\x1B\[\d+(;\d+){0,2}m')
# File extensions for image mime types that are captured as attachments.
IMAGE_EXTENSIONS = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/svg+xml': 'svg',
}
# Allowed origins/domains
ALLOWED_ORIGINS = {
    'https://sigmundai.eu',
    'http://localhost:5000',
    'http://127.0.0.1:5000',
    'https://127.0.0.1:5000',
    'https://localhost:5000'
}

EXTENSION_LOADED_MESSAGE = f'''## Sigmund extension for Jupyter (v{__version__})

__Important__: By connecting your Python session to Sigmund, you give an artificial intelligence (AI) full access to your file system. You are fully responsible for all of the actions that the AI performs, including accidental file deletions. AI is a powerful tool. Use it responsibly and carefully.

To connect to Sigmund:
        
- Open <a href="https://sigmundai.eu" target="_blank">https://sigmundai.eu</a> in a browser and log in
- Run `%sigmund_connect` in a cell of this Jupyter notebook
'''
STARTED_LISTENING_MESSAGE = '''### Trying to connect …

Open https://sigmundai.eu in a browser and log in. Jupyter will automatically connect to Sigmund.

Waiting …
'''
STOPPED_LISTENING_MESSAGE = '''### Disconnecting …

Waiting …
'''
ALREADY_LISTENING_MESSAGE = 'The extension is already listening.'
NOT_LISTENING_MESSAGE = 'The extension is not listening. Run `%sigmund_connect` first.'
CLIENT_CONNECTED_MESSAGE = '''### Connected

You can now use the following magic commands (`%`) to communicate with Sigmund.

Send a single-line message:

<pre>
%sigmund "Can you run a hello world script?"
</pre>

Send a multiline message:
        
<pre>
%%sigmund
Can you plot a sine-way graph?
Please use `matplotlib`.
</pre>

Disconnect:
        
<pre>
%sigmund_disconnect
</pre>
'''
CLIENT_DISCONNECTED_MESSAGE = '''### Disconnected

To re-connect, run `%sigmund_connect`.
'''
NOT_CONNECTED_MESSAGE = 'No Sigmund web client is connected. Run %sigmund_connect (if needed), then open https://sigmundai.eu in a browser and log in.'
SIGMUND_USAGE_MESSAGE = 'Usage: `%sigmund <message>` — or use `%%sigmund` as a cell magic, with the message in the body of the cell.'


class CommandError(Exception):
    """Raised when a command that was sent by Sigmund is invalid or cannot be
    executed. The error message is sent back to Sigmund as a tool result, so
    that Sigmund can correct itself.
    """
    
    
class MarkdownOutput:
    """A dummy class that shows HTML if supported and plain markdown otherwise.
    """
    def __init__(self, md):
        self._md = md
    
    def _repr_html_(self):
        return markdown(self._md,
                        extensions=[FencedCodeExtension(),
                                    CodeHiliteExtension()])
        
    def __repr__(self):
        md = self._md
        # Strip <pre></pre> tags around content
        if md.startswith('<pre>') and md.endswith('</pre>'):
            md = md[5:-6]
        return md


@magics_class
class WebSocketBridge(Magics):
    """A websocket server that allows the Sigmund web client to interact with
    this Python session.
    """

    def __init__(self, shell):
        super().__init__(shell)
        logger.debug('Initializing the WebSocketBridge')
        self.server = None
        self.clients = set()
        self.server_thread = None
        self.loop = None
        self.is_running = False
        self._attachments = []
        self._start_error = None
        # The Jupyter message (parent header) of the most recent %sigmund /
        # %%sigmund cell. This is used to attribute output that is generated
        # later on (often from the websocket server's background thread) to
        # the cell that actually triggered it. See _bind_parent_header.
        self._trigger_parent_header = None
        self._command_handlers = {
            'execute_code': self.run_command_ide_execute_code,
            'inspect_files': self.run_command_ide_inspect_files,
            'list_files': self.run_command_ide_list_files,
        }

    def _detect_notebook(self):
        """Detect if we're running in Jupyter notebook/lab vs QtConsole/IPython"""
        shell_name = self.shell.__class__.__name__
        logger.debug(f'Detected shell of type {shell_name!r}')
        if shell_name == 'ZMQInteractiveShell':
            # Check if we can display HTML
            try:
                from IPython.display import display, HTML
                display(HTML(""))  # Test if HTML display works
                logger.debug('HTML display works, running in a notebook')
                return True
            except Exception:
                logger.debug('HTML display does not work, running in a console')
                return False
        return False

    async def check_origin(self, websocket):
        """Only accept connections from the Sigmund web client"""
        origin = websocket.request.headers.get('Origin')
        if not origin:
            logger.error('Origin header missing')
            await websocket.close(code=1008, reason='Origin required')
            return False
        if origin not in ALLOWED_ORIGINS:
            logger.error(f'Unauthorized origin: {origin}')
            await websocket.close(code=1008, reason='Unauthorized origin')
            return False
        logger.debug(f'Allowed origin: {origin}')
        return True

    async def handle_client(self, websocket):
        """Handle incoming WebSocket connections"""
        logger.debug(
            f'Handling incoming connection from {websocket.remote_address}'
        )
        if self.clients:
            logger.warning(
                'A second client is trying to connect. Only one client can '
                'be connected at a time.'
            )
            await websocket.close()
            return
        # Check the origin before accepting the client
        if not await self.check_origin(websocket):
            return
        self.clients.add(websocket)
        logger.info(
            f'Sigmund web client connected from {websocket.remote_address} '
            f'({len(self.clients)} active client(s))'
        )
        self._display_markdown(CLIENT_CONNECTED_MESSAGE)
        try:
            # Identify ourselves as a Jupyter session.
            await websocket.send(json.dumps({
                'action': 'connector_name',
                'message': f'JupyterLab ({os.getpid()})'
            }))
            # Make sure that Sigmund doesn't use its own code-execution tool,
            # because code should be executed in the user's session instead.
            await websocket.send(json.dumps({
                'action': 'disable_code_execution'
            }))
            logger.debug(
                'Sent connector_name and disable_code_execution actions to '
                'the web client'
            )
            async for message in websocket:
                logger.debug(f'Received message: {_trunc(message)}')
                try:
                    data = json.loads(message)
                    await self.process_message(data, websocket)
                except json.JSONDecodeError as e:
                    logger.error(f'Invalid JSON received: {e}')
                except Exception as e:
                    # Don't let a single bad message kill the connection
                    logger.error(
                        f'Error processing message: {e}', exc_info=True)
        except websockets.exceptions.ConnectionClosed:
            logger.debug('The connection to the web client was closed')
        except Exception as e:
            logger.error(f'Error handling client: {e}', exc_info=True)
        finally:
            self.clients.discard(websocket)
            self._display_markdown(CLIENT_DISCONNECTED_MESSAGE)

    async def process_message(self, data, websocket):
        """Process incoming messages according to the protocol"""
        if not isinstance(data, dict):
            logger.debug('Ignoring message that is not a JSON object')
            return
        action = data.get('action')
        if action != 'ai_message':
            # Other actions (clear_messages, ai_incoming, token,
            # cancel_message, etc.) are not relevant here.
            logger.debug(f'Ignoring message with action {action!r}')
            return
        if data.get('on_connect', False):
            # When the web client (re)connects, it first replays the recent
            # conversation history. This should not trigger any actions.
            logger.debug('Ignoring AI message that was replayed on connect')
            return
        await self._handle_ai_message(data, websocket)

    async def _handle_ai_message(self, data, websocket):
        """Handle an AI message, which is either a command or plain text"""
        message = data.get('message') or ''
        workspace_content = data.get('workspace_content') or ''
        # A command is a JSON object (with a 'command' key) in the workspace
        # content. Anything else is a text message, which is shown in the
        # notebook. A future version may insert text messages as new Markdown
        # cells below the cell with the user message.
        command = self._parse_command(workspace_content)
        # Bind all output that is generated below (plain-text messages,
        # executed code, printed output, displayed images, etc.) to the cell
        # that triggered it (i.e. the most recent %sigmund / %%sigmund
        # cell), rather than to whichever cell the kernel considers
        # "current" by default. Without this, output that is generated from
        # the websocket server's background thread would be attributed to
        # whichever cell was running when that thread was started (i.e. the
        # cell where %sigmund_connect was run).
        with self._bind_parent_header(self._trigger_parent_header):
            if message.strip():
                self._display_markdown(message)
            elif command is None and workspace_content.strip():
                self._display_markdown(workspace_content)
            if command is None:
                logger.debug(
                    'The AI message is plain text (not a command): '
                    f'{_trunc(message or workspace_content)}'
                )
                return
            command_name, kwargs = command
            logger.info(f'Received command {command_name!r} from Sigmund')
            logger.debug(f'Command arguments: {kwargs}')
            self._attachments = []
            try:
                result = self._run_command(command_name, kwargs)
            except CommandError as e:
                logger.warning(
                    f'The command {command_name!r} could not be executed: {e}'
                )
                result = (
                    f'The command {command_name!r} could not be executed: {e}'
                )
            except Exception:
                logger.exception(
                    f'An unexpected error occurred while executing the '
                    f'command {command_name!r}'
                )
                result = (
                    'An unexpected error occurred while executing the '
                    f'command {command_name!r}:\n\n{traceback.format_exc()}'
                )
            else:
                logger.info(
                    f'The command {command_name!r} executed successfully'
                )
        # The result is sent back to Sigmund as a tool result. The empty
        # workspace content clears the workspace of the web client, so that
        # the same command is not executed a second time.
        self.send_user_message(
            TOOL_RESULT_PREFIX + result,
            attachments=self._attachments or None
        )

    def _parse_command(self, workspace_content):
        """Parse the workspace content as a command, if it is one.

        A command is a JSON object with a 'command' key. Returns a
        (command_name, kwargs) tuple, or None if the workspace content is not
        a command.
        """
        if not isinstance(workspace_content, str) \
                or not workspace_content.strip():
            return None
        try:
            cmd = json.loads(workspace_content)
        except json.JSONDecodeError:
            logger.debug('The workspace content is not valid JSON')
            return None
        if not isinstance(cmd, dict) or 'command' not in cmd:
            logger.debug('The workspace content is JSON, but not a command')
            return None
        command_name = cmd['command']
        kwargs = {key: value for key, value in cmd.items() if key != 'command'}
        return command_name, kwargs

    def _run_command(self, command_name, kwargs):
        """Execute a command that was sent by Sigmund.

        The arguments of the command may be wrapped in a subfield (e.g.
        'args' or 'kwargs'), in which case they are unwrapped first.
        """
        handler = self._command_handlers.get(command_name)
        if handler is None:
            supported = ', '.join(self._command_handlers)
            raise CommandError(
                f'Unknown command: {command_name!r}. Supported commands are: '
                f'{supported}.'
            )
        for key in ARG_WRAPPER_KEYS:
            if key not in kwargs:
                continue
            wrapped = kwargs.pop(key)
            if isinstance(wrapped, dict):
                kwargs.update(wrapped)
            elif isinstance(wrapped, (list, tuple)):
                # Positional arguments are mapped onto the argument names of
                # the command.
                arg_names = list(inspect.signature(handler).parameters)
                for arg_name, value in zip(arg_names, wrapped):
                    kwargs.setdefault(arg_name, value)
            elif wrapped is not None:
                raise CommandError(
                    f'Invalid arguments for command {command_name!r}: '
                    'expected a dictionary or list of arguments.'
                )
        logger.debug(
            f'Executing command {command_name!r} with arguments: {kwargs}'
        )
        try:
            return handler(**kwargs)
        except TypeError as e:
            raise CommandError(
                f'Invalid arguments for command {command_name!r}: {e}'
            ) from e

    def run_command_ide_execute_code(self, code=None, language='python'):
        """Execute Python code in the user's session (ide_execute_code).

        The code and its output are shown in the notebook, and the output is
        sent back to Sigmund, together with any images (plots, etc.) that the
        code generated.
        """
        if not isinstance(code, str) or not code.strip():
            raise CommandError(
                "The 'code' argument (the code to execute) is missing or empty."
            )
        if not isinstance(language, str) or not language.strip():
            language = 'python'
        if language.lower() != 'python':
            raise CommandError(
                f'Only Python code can be executed, not {language!r}.'
            )
        logger.info(
            f'Executing code in the user session '
            f'({len(code.splitlines())} line(s))'
        )
        logger.debug(f'Code to execute:\n{code}')
        self._display_markdown(f'```python\n{code}\n```')
        # Capture stdout and stderr, while also passing the output through to
        # the notebook or console.
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        original_stdout_write = sys.stdout.write
        original_stderr_write = sys.stderr.write

        def tee_write(capture, original_write):
            def write(string):
                capture.write(string)
                return original_write(string)
            return write

        try:
            sys.stdout.write = tee_write(stdout_capture, original_stdout_write)
            sys.stderr.write = tee_write(stderr_capture, original_stderr_write)
        except (AttributeError, TypeError):
            # Some environments don't allow monkey-patching the streams; in
            # that case the output is not captured, but it is still shown.
            logger.warning('Could not capture stdout and stderr.')
        # Capture images (plots, etc.) that are displayed by the code, so that
        # they can be sent to Sigmund as attachments.
        attachments = []
        original_publish = self.shell.display_pub.publish

        def capture_publish(data, metadata=None, source=None, **kwargs):
            for mime_type, content in (data or {}).items():
                if mime_type not in IMAGE_EXTENSIONS:
                    continue
                if isinstance(content, bytes):
                    encoded = base64.b64encode(content).decode('utf-8')
                else:
                    encoded = content
                ext = IMAGE_EXTENSIONS[mime_type]
                attachments.append({
                    'filename': f'output_{len(attachments) + 1}.{ext}',
                    'mime_type': mime_type,
                    'data': encoded
                })
                logger.debug(f'Captured {mime_type} display data')
            # Call the original publish method to ensure that the image is
            # also displayed in the notebook.
            return original_publish(data, metadata, source, **kwargs)

        self.shell.display_pub.publish = capture_publish
        logger.debug('Capturing stdout, stderr, and displayed images')
        try:
            result = self.shell.run_cell(
                code, silent=False, store_history=True)
        finally:
            # Restore the original write and publish methods
            for stream in (sys.stdout, sys.stderr):
                try:
                    del stream.write
                except AttributeError:
                    pass
            self.shell.display_pub.publish = original_publish
        # Collect the output
        output_parts = []
        stdout_text = stdout_capture.getvalue()
        if stdout_text:
            output_parts.append(stdout_text.rstrip())
        stderr_text = stderr_capture.getvalue()
        if stderr_text:
            output_parts.append(stderr_text.rstrip())
        # Handle errors (both syntax and runtime errors)
        if not result.success:
            logger.warning('Code execution failed')
            output_parts.append('\nAn error occurred during execution:\n')
            if result.error_before_exec is not None:
                output_parts.append(
                    f'SyntaxError: {result.error_before_exec}')
            elif result.error_in_exec is not None:
                # Use IPython's formatted traceback
                etype, value, tb = sys.exc_info()
                if etype is None:
                    # If sys.exc_info() doesn't have the info, construct it
                    etype = type(result.error_in_exec)
                    value = result.error_in_exec
                    tb = None
                formatted_tb = self.shell.InteractiveTB.structured_traceback(
                    etype, value, tb, tb_offset=0)
                tb_text = '\n'.join(formatted_tb)
                if tb_text and tb_text not in output_parts:
                    output_parts.append(tb_text)
        # Add the result value if there was no other output
        elif result.result is not None and not stdout_text:
            output_parts.append(repr(result.result))
        output_text = '\n'.join(output_parts) \
            if output_parts else '(no text output)'
        # Strip ansi escape sequences, which IPython may insert
        output_text = ANSI_PATTERN.sub('', output_text)
        self._attachments = attachments
        if attachments:
            logger.info(
                f'The code generated {len(attachments)} image(s), which are '
                f'captured as attachments for Sigmund'
            )
        logger.debug(f'Captured output:\n{output_text}')
        return (
            'The following code was executed and generated the output shown '
            'below:\n'
            f'<executed_code>\n{code}\n</executed_code>\n\n'
            f'<output>\n{output_text}\n</output>'
        )

    def run_command_ide_inspect_files(self, paths=None, encoding='utf-8'):
        """Return the contents of one or more files (ide_inspect_files)."""
        if isinstance(paths, str):
            paths = [paths]
        if not isinstance(paths, (list, tuple)) or not paths:
            raise CommandError(
                "The 'paths' argument (a list of files to inspect) is missing "
                'or empty.'
            )
        if not isinstance(encoding, str) or not encoding.strip():
            encoding = 'utf-8'
        logger.info(f'Inspecting {len(paths)} file(s)')
        blocks = []
        for path in paths:
            if not isinstance(path, str):
                logger.warning(f'Invalid (non-string) path: {path!r}')
                blocks.append(f'<file path="{path}" error="Invalid path" />')
                continue
            logger.debug(f'Inspecting file: {path}')
            try:
                with open(path, encoding=encoding) as fd:
                    content = fd.read()
            except FileNotFoundError:
                logger.warning(f'File not found: {path}')
                blocks.append(
                    f'<file path="{path}" error="File not found" />')
                continue
            except Exception as e:
                logger.warning(f'Could not read {path}: {e}')
                blocks.append(f'<file path="{path}" error="{e}" />')
                continue
            logger.debug(
                f'Successfully inspected {path} ({len(content)} characters)')
            blocks.append(f'<file path="{path}">\n{content}\n</file>')
        return '\n\n'.join(blocks)

    def run_command_ide_list_files(self, path=None, recursive=False,
                                   max_files=250):
        """Return a list of the files in a directory (ide_list_files)."""
        if not isinstance(path, str) or not path.strip():
            raise CommandError(
                "The 'path' argument (the directory to list) is missing or "
                'empty.'
            )
        path = path.strip()
        if isinstance(recursive, str):
            recursive = recursive.strip().lower() in ('true', '1', 'yes')
        try:
            max_files = int(max_files)
        except (TypeError, ValueError):
            max_files = 250
        max_files = max(1, max_files)
        logger.info(
            f'Listing files in {path} (recursive={recursive}, '
            f'max_files={max_files})'
        )
        if not os.path.exists(path):
            logger.warning(f'Directory not found: {path}')
            return f'Directory not found: {path}'
        if not os.path.isdir(path):
            logger.warning(f'Not a directory: {path}')
            return f'Not a directory: {path}'
        entries = []
        try:
            if recursive:
                for root, dirs, files in os.walk(path):
                    rel_root = os.path.relpath(root, path)
                    prefix = '' if rel_root == '.' else rel_root + os.sep
                    for dirname in dirs:
                        entries.append(prefix + dirname + os.sep)
                    for filename in files:
                        entries.append(prefix + filename)
            else:
                with os.scandir(path) as dir_iterator:
                    for entry in dir_iterator:
                        try:
                            is_dir = entry.is_dir()
                        except OSError:
                            is_dir = False
                        entries.append(
                            entry.name + (os.sep if is_dir else ''))
        except PermissionError:
            logger.warning(f'Permission denied: {path}')
            return f'Permission denied: {path}'
        entries.sort()
        if not entries:
            logger.debug(f'The directory is empty: {path}')
            return f'The directory is empty: {path}'
        if len(entries) > max_files:
            logger.debug(
                f'Found {len(entries)} entries in {path}, truncated the '
                f'listing at {max_files} entries'
            )
            return '\n'.join(
                entries[:max_files] + [f'(truncated at {max_files} entries)'])
        logger.debug(f'Found {len(entries)} entries in {path}')
        return '\n'.join(entries)

    def _capture_parent_header(self):
        """Return the Jupyter message that the kernel currently considers
        "active" (its parent header), or None if it cannot be determined
        (e.g. because we are not running inside a real Jupyter kernel).

        This is used to remember which cell triggered a %sigmund message, so
        that output that is generated later on (typically from the
        websocket server's background thread, long after the triggering
        cell has finished executing) can still be attributed to that same
        cell. See _bind_parent_header.
        """
        kernel = getattr(self.shell, 'kernel', None)
        if kernel is None:
            return None
        get_parent = getattr(kernel, 'get_parent', None)
        if callable(get_parent):
            try:
                return get_parent(channel='shell')
            except TypeError:
                try:
                    return get_parent('shell')
                except Exception:
                    logger.debug(
                        'Could not capture the parent header', exc_info=True)
            except Exception:
                logger.debug(
                    'Could not capture the parent header', exc_info=True)
        parent_header = getattr(kernel, '_parent_header', None)
        if isinstance(parent_header, dict) and 'shell' in parent_header:
            return parent_header['shell']
        return parent_header

    @contextmanager
    def _bind_parent_header(self, parent_header):
        """Temporarily associate all output (print statements, displayed
        images, executed code, etc.) with a specific Jupyter message, so
        that the frontend attributes it to the correct cell.

        Without this, output that is generated from the websocket server's
        background thread gets attributed to whichever cell the kernel
        considers "current", which by default is the cell that started the
        background thread (i.e. the cell where %sigmund_connect was run), rather
        than the cell that actually
        triggered the output (i.e. the most recent %sigmund cell).
        """
        set_parent = getattr(self.shell, 'set_parent', None)
        if not parent_header or not callable(set_parent):
            yield
            return
        previous_header = self._capture_parent_header()
        try:
            set_parent(parent_header)
        except Exception:
            logger.debug('Could not bind the parent header', exc_info=True)
            yield
            return
        try:
            yield
        finally:
            try:
                if previous_header:
                    set_parent(previous_header)
            except Exception:
                logger.debug(
                    'Could not restore the previous parent header',
                    exc_info=True
                )
            
    def _display_markdown(self, md):
        display(MarkdownOutput(md))

    def send_user_message(self, message, attachments=None, workspace_content='',
                          workspace_language=''):
        """Send a user message to Sigmund through the web client.

        This is thread-safe: it can be called from the kernel thread (i.e.
        from magic commands) as well as from the websocket thread.
        """
        if not self.clients or self.loop is None:
            logger.warning(
                'Cannot send a message, because no Sigmund web client is '
                'connected'
            )
            display(HTML("<b>Not connected!</b>"))
            self._display_markdown(NOT_CONNECTED_MESSAGE)
            return False
        data = {
            'action': 'user_message',
            'message': message,
            'workspace_content': workspace_content,
            'workspace_language': workspace_language,
            'transient_settings': TRANSIENT_SETTINGS,
            'transient_system_prompt': TRANSIENT_SYSTEM_PROMPT,
        }
        if attachments:
            data['attachments'] = attachments
        payload = json.dumps(data)
        if message.startswith(TOOL_RESULT_PREFIX):
            logger.info(
                f'Sending a tool result back to Sigmund: {_trunc(message)}'
            )
        else:
            logger.info(
                f'Sending a user message to the Sigmund web client: '
                f'{_trunc(message)}'
            )
        if attachments:
            logger.debug(
                f'The message includes {len(attachments)} attachment(s)'
            )
        for client in list(self.clients):
            try:
                asyncio.run_coroutine_threadsafe(
                    client.send(payload), self.loop)
            except Exception as e:
                logger.error(f'Failed to send message: {e}')
        return True

    def _start_server(self, host, port):
        """Start the websocket server on a separate thread"""
        if self.is_running:
            logger.debug('The websocket server is already running')
            self._display_markdown(ALREADY_LISTENING_MESSAGE)
            return
        logger.info(f'Starting the websocket server on {host}:{port}')
        self._start_error = None
        self.server_thread = Thread(
            target=self._run_server, args=(host, port), daemon=True)
        self.server_thread.start()
        # Wait (for at most 5 seconds) until the server is ready.
        for _ in range(100):
            if self.is_running or self._start_error is not None:
                break
            time.sleep(0.05)
        if self._start_error is not None:
            logger.error(f'Failed to start the server: {self._start_error}')
            self._display_markdown(f'Failed to start the server: {self._start_error}')
        elif not self.is_running:
            logger.error('Failed to start the server for an unknown reason.')
            self._display_markdown('Failed to start the server for an unknown reason.')

    def _run_server(self, host, port):
        """Run the websocket server on its own event loop.

        This is executed on a separate thread, so that the kernel remains
        responsive.
        """
        logger.debug('Starting the websocket server thread with a new event loop')
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        async def serve():
            try:
                self.server = await websockets.serve(
                    self.handle_client, host, port, ping_interval=20,
                    ping_timeout=10)
                self.is_running = True
                logger.info(f'Websocket server listening on ws://{host}:{port}')
                # Keep the server running until it is closed
                await self.server.wait_closed()
            except Exception as e:
                self._start_error = str(e)
                logger.error(f'Failed to start the server: {e}', exc_info=True)

        try:
            self.loop.run_until_complete(serve())
        except Exception as e:
            logger.error(f'Server error: {e}', exc_info=True)
        finally:
            self.is_running = False
            logger.debug('The websocket server has stopped')
            try:
                self.loop.close()
            except Exception:
                pass

    @line_magic
    @magic_arguments()
    @argument('--port', type=int, default=8080,
              help='WebSocket server port (default: 8080)')
    @argument('--host', default='localhost', help='WebSocket server host')
    def sigmund_connect(self, line):
        """Start listening for a connection from the Sigmund web client"""
        self._display_markdown(STARTED_LISTENING_MESSAGE)
        args = parse_argstring(self.sigmund_connect, line)
        logger.info(
            f'%sigmund_connect called (host={args.host}, port={args.port})'
        )
        self._start_server(args.host, args.port)

    @line_magic
    def sigmund_disconnect(self, line):
        """Stop the WebSocket bridge server"""
        if not self.is_running:
            logger.debug(
                '%sigmund_disconnect was called, but the extension is not '
                'listening'
            )
            return
        logger.info('Stopping the websocket server')
        server = self.server
        loop = self.loop
        self.server = None
        self.is_running = False
        if server is not None and loop is not None and loop.is_running():
            close = server.close()
            if asyncio.iscoroutine(close):
                # In recent versions of websockets, close() is a coroutine
                # that is executed on the server's event loop. In older
                # versions, close() is a regular method and the server has
                # already been closed.
                asyncio.run_coroutine_threadsafe(close, loop)
                logger.debug('Requested the websocket server to close')
        self._display_markdown(STOPPED_LISTENING_MESSAGE)

    @line_cell_magic
    def sigmund(self, line='', cell=None):
        """Send a message to Sigmund.

        This can be used as a line magic (`%sigmund <message>`) or as a cell
        magic (`%%sigmund` with the message in the body of the cell).
        """
        if cell is None:
            message = line
        elif line.strip():
            message = f'{line.strip()}\n\n{cell}'
        else:
            message = cell
        message = message.strip()
        if not message:
            logger.debug('%sigmund was called without a message')
            self._display_markdown(SIGMUND_USAGE_MESSAGE)
            return
        if not self.is_running:
            logger.warning(
                '%sigmund was called, but the extension is not listening'
            )
            self._display_markdown(NOT_LISTENING_MESSAGE)
            return
        # Remember which cell this is, so that any output that Sigmund
        # generates later on (executed code, printed output, displayed
        # images, etc.), possibly from the websocket server's background
        # thread, can be attributed to this same cell rather than to
        # whichever cell the kernel considers "current" by default.
        self._trigger_parent_header = self._capture_parent_header()
        logger.debug(
            f'Sending a message to Sigmund via %sigmund: {_trunc(message)}'
        )
        self.send_user_message(message)


# Global instance to maintain state
_bridge_instance = None


def load_ipython_extension(ipython):
    """Load the extension"""
    global _bridge_instance
    logger.info(f'Loading the SigmundAI extension for Jupyter (v{__version__})')
    _bridge_instance = WebSocketBridge(ipython)
    ipython.register_magic_function(_bridge_instance.sigmund_connect)
    ipython.register_magic_function(_bridge_instance.sigmund_disconnect)
    ipython.register_magic_function(
        _bridge_instance.sigmund, 'line_cell', 'sigmund')
    logger.debug(
        'Registered magics: %sigmund_connect, '
        '%sigmund_disconnect, %sigmund'
    )
    _bridge_instance._display_markdown(EXTENSION_LOADED_MESSAGE)


def unload_ipython_extension(ipython):
    """Unload the extension"""
    global _bridge_instance
    logger.info('Unloading the SigmundAI extension for Jupyter')
    if _bridge_instance is not None and _bridge_instance.is_running:
        _bridge_instance.sigmund_disconnect('')
    _bridge_instance._display_markdown('SigmundAI extension for Jupyter unloaded')
    _bridge_instance = None
