#!/bin/bash

set -x

kubectl delete -f https://github.com/jetstack/cert-manager/releases/download/v1.6.1/cert-manager.yaml
