ARG PYTHON_VERSION

FROM mcr.microsoft.com/vscode/devcontainers/python:${PYTHON_VERSION}
ARG POETRY_VERSION

USER vscode
RUN python -m pip install --user --upgrade --no-cache-dir poetry==${POETRY_VERSION}
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
RUN echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' | sudo tee /etc/apt/sources.list.d/google-chrome.list
RUN sudo apt-get update && sudo apt-get -y install google-chrome-stable chromium-driver
