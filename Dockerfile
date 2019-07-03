FROM ubuntu

ENV HOME="/root"

RUN mkdir $HOME/src
VOLUME $HOME/src

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
      curl \
      python3.7 \
      python3-pip

WORKDIR $HOME/src
