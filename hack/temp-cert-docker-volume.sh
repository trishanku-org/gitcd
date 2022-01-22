#!/bin/bash

VOL_NAME="$1"
BINDIR="$2"
BINDIR_ABS=$(realpath "$BINDIR")

docker volume create "${VOL_NAME}"
docker run --rm -v "${BINDIR_ABS}:/bindir" -v "${VOL_NAME}:/certsdir" busybox cp /bindir/server.crt /bindir/server.key /certsdir/
docker run --rm -v "${VOL_NAME}:/certsdir" busybox ln -s /certsdir/server.crt /certsdir/ca-certificates.crt