FROM python:3.12.2-slim-bullseye

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
