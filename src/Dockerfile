
# test_BUILD_OPTS --build-arg use_image=testing
ARG use_image=base

################
# Base image
FROM python:3.13.3-slim-bullseye AS base

# Work directory
RUN mkdir -p /work && chmod 777 /work
WORKDIR /work
ENV HOME=/work

# Install any OS packages
ARG DEBIAN_FRONTEND=noninteractive
# RUN apt-get update && apt-get install -yq --no-install-recommends sshpass
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install any python packages
COPY requirements.txt /work
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r /work/requirements.txt

COPY entrypoint /entrypoint
ENTRYPOINT [ "/entrypoint" ]

################
# Testing image
FROM base AS testing

COPY tests /work/tests

COPY tests/entrypoint /test-entrypoint
ENTRYPOINT [ "/test-entrypoint" ]

COPY tests/requirements.txt /work/test-requirements.txt
RUN python3 -m pip install -r /work/test-requirements.txt

################
# Final image
FROM ${use_image}

# Copy kmt module
RUN mkdir /work/modules
ENV PYTHONPATH=/work/modules
COPY kmt /work/modules/kmt/

# Save build version
ARG VERSION
RUN echo "${VERSION}" > /work/build_version.txt
RUN echo "$(date --iso-8601=seconds)" > /work/build_date.txt

