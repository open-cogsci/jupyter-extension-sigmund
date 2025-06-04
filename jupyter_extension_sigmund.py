"""This is an extension for Jupyterlab, Jupyter Notebook, Spyder, Rapunzel, or any other application that uses a Jupyter/ IPython based console. It allows you to connect your Python session to [SigmundAI](https://sigmundai.eu). This is mainly intended as a tool for coding and data analysis.

__IMPORTANT__: By connecting your Python session to Sigmund, you give an artificial intelligence (AI) full access to your file system. You are fully responsible for all of the actions that the AI performs, including accidental file deletions. AI is a powerful tool. Use it responsibly and carefully.
"""
import asyncio
import websockets
import json
import sys
import re
from IPython.core.magic import Magics, magics_class, line_magic
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.display import display, HTML, Code
from threading import Thread
import logging
from io import StringIO
import contextlib
import time


logger = logging.getLogger(__name__)
__version__ = '0.1.0'
STARTUP_DELAY = 5  # seconds
SIGMUND_INSTRUCTIONS = "I connected you to Jupyter. Any code that you put in the workspace will now automatically be executed, and then I will send you the output back. Please confirm that you understand how to execute code now? (But don't actually execute any code yet!"
EXTENSION_LOADED_MESSAGE = '''SigmundAI extension for Jupyter loaded. To connect to Sigmund, run %start_listening.

IMPORTANT: By connecting your Python session to Sigmund, you give an artificial intelligence (AI) full access to your file system. You are fully responsible for all of the actions that the AI performs, including accidental file deletions. AI is a powerful tool. Use it responsibly and carefully.
'''
STOPPED_LISTENING_MESSAGE = 'Stopped listening. To connect to Sigmund, run %start_listening.'
STARTED_LISTENING_MESSAGE = 'Started listening. Open https://sigmundai.eu in a browser and log in. You will automatically connect once the browser tab is loaded.'
CLIENT_CONNECTED_MESSAGE = 'Connected to SigmundAI. To disconnect, run %stop_listening.'
# Allowed origins/domains
ALLOWED_ORIGINS = {
    'https://sigmundai.eu',
    'http://localhost:5000',
    'http://127.0.0.1:5000',
    'https://127.0.0.1:5000',
    'https://localhost:5000'
}



@magics_class
class WebSocketBridge(Magics):
    def __init__(self, shell):
        super().__init__(shell)
        self.server = None
        self.clients = set()
        self.server_thread = None
        self.loop = None
        self.is_running = False
        self.is_notebook = self._detect_notebook()
        
    def _detect_notebook(self):
        """Detect if we're running in Jupyter notebook/lab vs QtConsole/IPython"""
        shell_name = self.shell.__class__.__name__
        if shell_name == 'ZMQInteractiveShell':
            # Check if we can display HTML
            try:
                from IPython.display import display, HTML
                display(HTML(""))  # Test if HTML display works
                return True
            except Exception:
                return False
        return False
        
    async def check_origin(self, websocket):
        origin = websocket.request.headers.get('Origin')        
        if not origin:
            logger.error("Origin header missing")
            await websocket.close(code=1008, reason="Origin required")
            return False        
        if origin not in ALLOWED_ORIGINS:
            logger.error(f"Unauthorized origin: {origin}")
            await websocket.close(code=1008, reason="Unauthorized origin")
            return False        
        logger.debug(f"Allowed origin: {origin}")
        return True        
        
    async def handle_client(self, websocket):
        """Handle incoming WebSocket connections"""
        if self.clients:
            logger.warning('A second client is trying to connect. Only one client can be connected a time.')
            await websocket.close()
            return
        # Check origin first
        if not await self.check_origin(websocket):
            return        
        
        self.clients.add(websocket)
        print(CLIENT_CONNECTED_MESSAGE)
        
        # Send response according to protocol
        response = {"action": "disable_code_execution"}
        await websocket.send(json.dumps(response))
        # Send response according to protocol
        response = {
            "action": "connector_name",
            "message": f"Python {sys.version}"
        }        
        await websocket.send(json.dumps(response))
               
        # Send response according to protocol
        response = {
            "action": "user_message",
            "message": SIGMUND_INSTRUCTIONS,
            "workspace_content": "",
            "workspace_language": ""
        }
        
        await websocket.send(json.dumps(response))        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.process_message(data, websocket)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON received: {e}")
                    # Don't send error response as it's not part of the protocol
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            self.clients.discard(websocket)
            disconnect_info = f"Client disconnected. Total: {len(self.clients)}"
            logger.debug(disconnect_info)
            
            if self.shell.__class__.__name__ == 'ZMQInteractiveShell':
                display(HTML(f"<div style='color: orange;'>{disconnect_info}</div>"))
 
    async def process_message(self, data, websocket):
        """Process incoming messages according to the protocol"""
        action = data.get('action')
        on_connect = data.get('on_connect', False)
        workspace_content = data.get('workspace_content', '')
        # Only process ai_message with content and not on_connect
        if action == 'ai_message' and workspace_content and not on_connect:
            await self.execute_and_respond(data, websocket)
    
    async def execute_and_respond(self, data, websocket):
        """Execute code and send response according to protocol"""
        code = data.get('workspace_content', '')
        message = data.get('message')
        display(HTML(message))
        display(Code(code))
        workspace_language = data.get('workspace_language', 'python')
    
        # Capture stdout/stderr, but also redirect to original so that we don't
        # hide text output in Jupyter
        def splitter(fnc1, fnc2):
            def inner(*args, **kwargs):
                fnc1(*args, **kwargs)
                fnc2(*args, **kwargs)
            return inner
        stdout_capture = StringIO()
        stdout_capture.write = splitter(stdout_capture.write,
                                        sys.__stdout__.write)
        stderr_capture = StringIO()
        stderr_capture.write = splitter(stderr_capture.write,
                                        sys.__stderr__.write)
        attachments = []
        
        # Store display data
        captured_displays = []
        
        # Monkey-patch the publish method to capture display data
        original_publish = self.shell.display_pub.publish
        
        def capture_publish(data, metadata=None, source=None, **kwargs):
            # Capture display data
            if data:
                captured_displays.append(data.copy())
                
                for mime_type, content in data.items():
                    if mime_type in ['image/png', 'image/jpeg', 'image/svg+xml']:
                        # Convert to base64 if needed
                        if isinstance(content, bytes):
                            import base64
                            encoded = base64.b64encode(content).decode('utf-8')
                        else:
                            encoded = content
                        
                        # Generate filename
                        ext = mime_type.split('/')[-1]
                        filename = f"output_{len(attachments) + 1}.{ext}"
                        
                        attachments.append({
                            "filename": filename,
                            "mime_type": mime_type,
                            "data": encoded
                        })
                        
                        logger.debug(f"Captured {mime_type} display data: {filename}")
            
            # Call original to ensure display in notebook
            return original_publish(data, metadata, source, **kwargs)
        
        # Temporarily replace the publish method
        self.shell.display_pub.publish = capture_publish
        
        try:
            with contextlib.redirect_stdout(stdout_capture), \
                 contextlib.redirect_stderr(stderr_capture):
                # Execute the code
                result = self.shell.run_cell(code, silent=False, store_history=True)
        finally:
            # Restore original publish method
            self.shell.display_pub.publish = original_publish
    
        # Collect output
        output_parts = []
        
        # Add stdout
        stdout_text = stdout_capture.getvalue()
        if stdout_text:
            output_parts.append(stdout_text.rstrip())
    
        # Add stderr  
        stderr_text = stderr_capture.getvalue()
        if stderr_text:
            output_parts.append(stderr_text.rstrip())    
    
        # Handle errors (both syntax and runtime)
        if not result.success:
            logger.debug('execution failed')
            output_parts.append('\nAn error occurred during execution:\n')
            # Check for syntax errors first
            if result.error_before_exec is not None:
                output_parts.append(f'SyntaxError: {result.error_before_exec}')        
            elif result.error_in_exec is not None:
                # Runtime error occurred during execution
                # Use IPython's formatted traceback
                etype, value, tb = sys.exc_info()
                if etype is None:
                    # If sys.exc_info() doesn't have the info, construct it
                    etype = type(result.error_in_exec)
                    value = result.error_in_exec
                    tb = None
                # Get formatted traceback from IPython
                formatted_tb = self.shell.InteractiveTB.structured_traceback(
                    etype, value, tb, tb_offset=0
                )
                tb_text = '\n'.join(formatted_tb)            
                if tb_text and tb_text not in output_parts:
                    output_parts.append(tb_text)
        # Add result value if present
        elif result.result is not None and not stdout_text:
            output_parts.append(repr(result.result))
    
        # Combine all output
        output_text = "\n".join(output_parts) if output_parts else "(no text output)"
        # Strip ansi escape sequences, which IPython may insert
        pattern = re.compile(r'\x1B\[\d+(;\d+){0,2}m')
        output_text = pattern.sub('', output_text)
        message_content = f'I executed the code in the workspace, and received the following output:\n\n{output_text}'
        
        # Send response according to protocol
        response = {
            "action": "user_message",
            "message": message_content,
            "workspace_content": code,
            "workspace_language": workspace_language
        }        
        # Add attachments if any
        if attachments:
            response["attachments"] = attachments
        
        await websocket.send(json.dumps(response))
    
    @line_magic
    @magic_arguments()
    @argument('--port', type=int, default=8080, help='WebSocket server port (default: 8080)')
    @argument('--host', default='localhost', help='WebSocket server host')
    @argument('--verbose', action='store_true', help='Enable verbose logging')
    def start_listening(self, line):
        """Start the WebSocket bridge server"""
        print(STARTED_LISTENING_MESSAGE)
        args = parse_argstring(self.start_listening, line)
        
        if self.is_running:            
            return
            
        # Start the WebSocket server in a separate thread
        def run_server():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            async def start_server_async():
                self.server = await websockets.serve(
                    self.handle_client, 
                    args.host, 
                    args.port,
                    ping_interval=20,
                    ping_timeout=10
                )
                self.is_running = True                
                # Keep server running
                await self.server.wait_closed()
            
            try:
                self.loop.run_until_complete(start_server_async())
            except Exception as e:
                logger.error(f"Server error: {e}")
                self.is_running = False
        
        self.server_thread = Thread(target=run_server, daemon=True)
        self.server_thread.start()
        
        # Give the server a moment to start
        time.sleep(STARTUP_DELAY)

    
    @line_magic
    def stop_listening(self, line):
        """Stop the WebSocket bridge server"""
        # if self.loop and self.server:
            # asyncio.run_coroutine_threadsafe(self.server.close(), self.loop)
        self.server.close()
        self.is_running = False
        self.server = None
        print(STOPPED_LISTENING_MESSAGE)


# Global instance to maintain state
_bridge_instance = None

def load_ipython_extension(ipython):
    """Load the extension"""
    global _bridge_instance
    _bridge_instance = WebSocketBridge(ipython)
    
    ipython.register_magic_function(_bridge_instance.start_listening)
    ipython.register_magic_function(_bridge_instance.stop_listening)
    
    print(EXTENSION_LOADED_MESSAGE)


def unload_ipython_extension(ipython):
    """Unload the extension"""
    global _bridge_instance
    if _bridge_instance and _bridge_instance.is_running:
        _bridge_instance.stop_bridge("")
    _bridge_instance = None
    print('SigmundAI extension for Jupyter unloaded')
