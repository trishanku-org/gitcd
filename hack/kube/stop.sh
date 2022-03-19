#!/bin/bash

set -x

kind delete cluster --name trishanku
docker stop kube-apiserver gitcd-main etcd-events gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
