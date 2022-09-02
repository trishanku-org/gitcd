#!/bin/bash

set -x

if ! docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git checkout refs/gitcd/metadata/refs/heads/main; then
    docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git reset --hard  && \
        docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git checkout refs/gitcd/metadata/refs/heads/main
fi

test $? -eq 0 && \
    docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git checkout main && \
    docker run -i --rm -v gitcd-backend:/backend -w /backend --entrypoint tar nginx:1 -cz . | dd of=bin/gitcd-backend.tar.gz
