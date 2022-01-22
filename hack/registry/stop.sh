#!/bin/bash

set -x

docker stop registry && \
    docker inspect registry > /dev/null 2>&1 && \
    docker rm registry