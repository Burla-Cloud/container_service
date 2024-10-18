
# This is the default docker image that all user-submitted functions are run in.
# This separate image inherits from `Dockerfile.base` so we can quickly deploy the container_service
# by baking it into & uploading a new image without needing to install python again

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

WORKDIR /burla
RUN rm pyproject.toml && rm -rf ./src
ADD ./src /burla/src
ADD pyproject.toml /burla

RUN /.pyenv/versions/3.10.*/bin/python3.10 -m pip install pandas numpy scikit-learn && \
    /.pyenv/versions/3.11.*/bin/python3.11 -m pip install pandas numpy scikit-learn && \
    /.pyenv/versions/3.12.*/bin/python3.12 -m pip install pandas numpy scikit-learn