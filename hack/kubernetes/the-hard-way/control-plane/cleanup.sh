#!/bin/bash

ETCD_CONTAINER_NAME=etcd
ETCD_VOLUME_NAME="kubernetes-the-hard-way-etcd"
KUBE_APISERVER_CONTAINER_NAME=kube-apiserver
KUBE_CONTROLLER_MANAGER_CONTAINER_NAME=kube-controller-manager
KUBE_SCHEDULER_CONTAINER_NAME=kube-scheduler
KUBE_SCHEDULER_VOLUME_NAME="kubernetes-the-hard-way-scheduler"

SOURCE_PATH=$(readlink -f "${BASH_SOURCE:-$0}")
DIRNAME=$(dirname "$SOURCE_PATH")

ls -1 "${DIRNAME}/configs/kube-controller-managers/" | sed 's/\(.*\)/'${KUBE_CONTROLLER_MANAGER_CONTAINER_NAME}'-\1/' | \
    xargs docker stop $KUBE_SCHEDULER_CONTAINER_NAME $KUBE_APISERVER_CONTAINER_NAME $ETCD_CONTAINER_NAME

ls -1 "${DIRNAME}/configs/kube-controller-managers/" | sed 's/\(.*\)/'${KUBE_CONTROLLER_MANAGER_CONTAINER_NAME}'-\1/' | \
    xargs docker rm $KUBE_SCHEDULER_CONTAINER_NAME $KUBE_APISERVER_CONTAINER_NAME $ETCD_CONTAINER_NAME

docker volume rm $KUBE_SCHEDULER_VOLUME_NAME $ETCD_VOLUME_NAME
