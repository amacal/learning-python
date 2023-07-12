ARG PYTHON_VERSION

FROM mcr.microsoft.com/vscode/devcontainers/python:${PYTHON_VERSION}
ARG POETRY_VERSION

USER vscode
RUN python -m pip install --user --upgrade --no-cache-dir poetry==${POETRY_VERSION}
