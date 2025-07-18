# Developers' Guidelines

This file contains a few useful project development tips (some generic, some project specific)

## Windows versus Unix-type Development

At the moment (June 2025), the project uses the [uv Python package and project manager](https://docs.astral.sh/uv/). 

To facilitate setup, testing and data generation, a Gnu-like Makefile environment was created. This kind of tool is obviously native to Unix environments and a bit more troublesome under Microsoft Windows unless one sets up a suitable Windows-compliant unix shell (e.g. CygWin?) - feasible but not less than ideal at times. 

The situation gets a bit more complex when Integrated Development Environments enter the picture.

### PyCharms under Windows

Here we discuss the tricky issues relating to the JetBrains PyCharms IDE operated under Microsoft Windows (e.g. release 11).  Most importantly, this discussion assumes that _you have Git cloned a single copy of the project code into one local directory jointly used by both Windows and Linux (WSL2)_.

First, yes... Windows has WSL2, which can host a Linux runtime operating system with Gnu **`make`** available for the project (e.g. like Ubuntu - setting this up is beyond the scope of this README: consult Microsoft documentation for that). 

Second, **uv** generally works equally well under native Windows and WSL2 Linux shells (it's Python, after all!). Thus, one can use **uv** within a commandline shell within a (WSL2 hosted) Linux to successfully install a virtual environment with all Python and the **`make`** tool, to **`make all`** of the project (as noted in [the main project README](./README.md)), however...

Third, the PyCharms IDE operation tends to favor the OS under which it is run (say, Windows 11). That is, package resolution and code navigation, Pytest, etc. generally needs to see a Windows virtual environment configured (i.e. by **uv**) with all required packages. _It can't reuse the Linux venv configured in the  by **uv** for this purpose._: running most **uv** commands in a Windows PowerShell with a default venv configured by Linux **uv** operations will fail.

A workable solution requires the following:
Within PyCharms settings..Tools..Terminal..Project Settings..Environment Variables, add 

**`UV_PROJECT_ENVIRONMENT=<name-of-windows-specific-venv-folder>** ` 

where the <name-of-windows-specific-venv-folder> is the actual directory name within which you wish to manage the Windows virtual environment.

Within the root project direction inside the Windows PowerShell, execute the following commands:

1. **`uv python install`** to ensure installation of a Windows-compatible Python interpreter
2. **`uv venv <name-of-windows-specific-venv-folder>`** to install a Windows-compatible virtual environment.
3. **`uv sync`** to install all the Python library dependencies specified in the project's current uv.lock

All of the above should create an environment visible to the Windows PyCharm IDE, while still allowing the parallel Ubuntu environment to run the system.
