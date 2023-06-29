FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y \
    net-tools iputils-ping python3 python3-pip git
RUN pip3 install finmind


