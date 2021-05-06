ARG ALPINE_VERSION="3.12"
ARG PYTHON_VERSION="3.9"
ARG BASE_IMAGE="python:${PYTHON_VERSION}-alpine${ALPINE_VERSION}"

# Stage 1 - bundle base image + runtime
FROM ${BASE_IMAGE} AS python-alpine 
RUN apk add \
    --no-cache \
    libstdc++ 

# Stage 2 - build function and dependencies
FROM python-alpine AS build-image

RUN apk add \
    --no-cache \ 
    build-base \
    libtool \
    autoconf \
    automake \
    libexecinfo-dev \
    make \
    cargo \
    rust \
    cmake \
    libcurl \
    libffi-dev \
    openssl-dev \
    groff 


RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" 
RUN python3 -m pip install awslambdaric==1.0.0 boto3==1.17

COPY . .
RUN python3 -m pip install -r requirements.txt

# Stage 3 - final runtime image
FROM python-alpine
WORKDIR /home

RUN apk add \
    --no-cache \
    openssl \
    openjdk8-jre

COPY --from=build-image /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY --from=build-image heracles ./heracles

#(Optional) Add Lambda Runtime Interface Emulator and use a script in the ENTRYPOINT for simpler local runs
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/bin/aws-lambda-rie 
COPY entry.sh /
RUN chmod 755 /usr/bin/aws-lambda-rie /entry.sh

ENTRYPOINT [ "/entry.sh" ]
CMD [ "heracles.lambda.handler" ]
