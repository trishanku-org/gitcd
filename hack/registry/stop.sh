#!/bin/bash

set -x

docker stop registry && \
    docker rm registry