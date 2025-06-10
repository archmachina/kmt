
# test_BUILD_OPTS --build-arg use_image=testing

ARG use_image=base
FROM python:3.13.3-slim-bullseye AS base

ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /work
ENV HOME=/work

COPY requirements.txt /work
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r /work/requirements.txt

COPY kmt /work/modules/kmt/
ENV PYTHONPATH=/work/modules

ARG BUILD_VERSION
RUN echo "${BUILD_VERSION}" > /build_version.txt

COPY entrypoint /entrypoint
ENTRYPOINT [ "/entrypoint" ]

FROM base AS testing

COPY tests /work/tests

COPY tests/entrypoint /test-entrypoint
ENTRYPOINT [ "/test-entrypoint" ]

COPY tests/requirements.txt /work/test-requirements.txt
RUN python3 -m pip install -r /work/test-requirements.txt

FROM ${use_image}

