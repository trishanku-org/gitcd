#!/bin/bash

set -x

kind delete cluster --name trishanku
docker stop etcd-events gitcd-main gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
