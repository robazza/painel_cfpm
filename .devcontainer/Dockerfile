FROM mcr.microsoft.com/devcontainers/python:3.7

#FROM mcr.microsoft.com/devcontainers/anaconda:0-3

ENV LD_LIBRARY_PATH=/opt/instantclient_19_19

# Copy environment.yml (if found) to a temp location so we update the environment. Also
# copy "noop.txt" so the COPY instruction does not fail if no environment.yml exists.
COPY environment.yml* .devcontainer/noop.txt /tmp/conda-tmp/

#somente rodar com CONDA
#RUN if [ -f "/tmp/conda-tmp/environment.yml" ]; then umask 0002 && /opt/conda/bin/conda env update -n base -f /tmp/conda-tmp/environment.yml; fi \
#    && rm -rf /tmp/conda-tmp

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
     && apt-get -y install --no-install-recommends libaio1


COPY .containerassets/instantclient-basiclite-linux.x64-19.19.0.0.0dbru.zip /opt/instantclient-basiclite-linux.x64-19.19.0.0.0dbru
COPY requirements.txt /opt/dash-app/requirements.txt

RUN pip install -r /opt/dash-app/requirements.txt \
     && unzip /opt/instantclient-basiclite-linux.x64-19.19.0.0.0dbru -d /opt \
	 && rm -rf /opt/instantclient-basiclite-linux.x64-19.19.0.0.0dbru