#!/bin/bash

set -x

function stop_containers {
    for container_name in "$@"; do
        docker stop "$container_name" && docker rm "$container_name"
    done
}

# kind delete cluster --name trishanku
stop_containers gitcd etcd-events # gitcd-main etcd-events gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
docker volume rm gitcd etcd-events # gitcd-main etcd-events gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
