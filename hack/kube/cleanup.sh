#!/bin/bash

set -x

docker stop kube-apiserver && docker rm kube-apiserver
docker stop gitcd && docker rm gitcd
docker volume rm kube-certs gitcd-backend
