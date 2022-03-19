#!/bin/bash

set -x

kind delete cluster --name trishanku
docker stop gitcd kube-apiserver
