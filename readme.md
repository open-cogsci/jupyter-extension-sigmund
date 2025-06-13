# Jupyter extension for SigmundAI

__Disclaimer: This is an early development preview__

This is an extension for Jupyterlab, Jupyter Notebook, Spyder, Rapunzel, or any other application that uses a Jupyter/ IPython based console. It allows you to connect your Python session to [SigmundAI](https://sigmundai.eu). This is mainly intended as a tool for AI-assisted coding and data analysis.

__IMPORTANT__: By connecting your Python session to Sigmund, you give an artificial intelligence (AI) full access to your file system. You are fully responsible for all of the actions that the AI performs, including accidental file deletions. AI is a powerful tool. Use it responsibly and carefully.

[output2.webm](https://github.com/user-attachments/assets/905233c3-5980-45f5-b8fb-dc769b4c3526)


## Installation

```
pip install jupyter-extension-sigmund
```

This extension will be updated often. Please make sure to update it regularly!

```
pip install jupyter-extension-sigmund --upgrade
```


## Usage

First load the extension with a magic command. This will work in any console that is based on Jupyter/IPython, including Jupyterlab, Jupyter Notebook, Spyder, and Rapunzel.

```
%load_ext jupyter_extension_sigmund
```

Next, start a new conversation with Sigmund with another magic command:

```
%start_listening
```

Once the extension is listening, all you need to do is open https://sigmundai.eu in a browser. A connection will then automatically be established.

If you want to resume a previous conversation, you can use yet another magic command: (The only difference is that this will not resend instructions to Sigmund.))

```
%resume_listening
```


## License

This software is licensed under the [GNU General Public License
v3](http://www.gnu.org/licenses/gpl-3.0.en.html).
