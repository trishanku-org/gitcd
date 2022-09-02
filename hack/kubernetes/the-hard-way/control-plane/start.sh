#!/bin/bash

SECRETS_VOLUME_NAME="kubernetes-the-hard-way-secrets"
ETCD_VOLUME_NAME="kubernetes-the-hard-way-etcd"
ETCD_NAME="kubernetes-the-hard-way"
ETCD_CONTAINER_NAME=etcd
ETCD_IMG=bitnami/etcd:3

KUBE_VERSION=v1.21.14
KUBECTL_IMG=bitnami/kubectl:1.21.14
KUBE_IMG_BASE=k8s.gcr.io
KUBE_APISERVER_IMG=${KUBE_IMG_BASE}/kube-apiserver:${KUBE_VERSION}
KUBE_CONTROLLER_MANAGER_IMG=${KUBE_IMG_BASE}/kube-controller-manager:${KUBE_VERSION}
KUBE_SCHEDULER_IMG=${KUBE_IMG_BASE}/kube-scheduler:${KUBE_VERSION}
KUBE_APISERVER_CONTAINER_NAME=kube-apiserver
KUBE_CONTROLLER_MANAGER_CONTAINER_NAME=kube-controller-manager
KUBE_SCHEDULER_CONTAINER_NAME=kube-scheduler
KUBE_SCHEDULER_VOLUME_NAME="kubernetes-the-hard-way-scheduler"

if [ "$LOCAL_IP" == "" ]; then
  LOCAL_IP="127.0.0.1"
fi

if [ "$INTERNAL_IP" == "" ]; then
  INTERNAL_IP="$(ifconfig | awk '/inet / {split($2,var,"/*"); print var[1]}' | grep -v 127.0.0.1 | head -n 1)"
fi

if  [ "$KUBERNETES_PUBLIC_ADDRESS" == "" ]; then
  KUBERNETES_PUBLIC_ADDRESS="$INTERNAL_IP"
fi

if docker inspect $ETCD_CONTAINER_NAME > /dev/null 2>&1; then
  echo "Container ${ETCD_CONTAINER_NAME} already exists."
else
  docker volume create $ETCD_VOLUME_NAME
  docker run --name $ETCD_CONTAINER_NAME \
    -d --restart=unless-stopped \
    -p 2379:2379 \
    -v "${SECRETS_VOLUME_NAME}:/secrets" \
    -v "${ETCD_VOLUME_NAME}:/var/lib/etcd" \
    -u 0 \
    --entrypoint "" \
    $ETCD_IMG etcd \
      --name ${ETCD_NAME} \
      --cert-file=/secrets/kubernetes.pem \
      --key-file=/secrets/kubernetes-key.pem \
      --peer-cert-file=/secrets/kubernetes.pem \
      --peer-key-file=/secrets/kubernetes-key.pem \
      --trusted-ca-file=/secrets/ca.pem \
      --peer-trusted-ca-file=/secrets/ca.pem \
      --peer-client-cert-auth \
      --client-cert-auth \
      --initial-advertise-peer-urls https://${LOCAL_IP}:2380 \
      --listen-peer-urls https://${LOCAL_IP}:2380 \
      --listen-client-urls https://0.0.0.0:2379 \
      --advertise-client-urls https://${LOCAL_IP}:2379 \
      --initial-cluster-token etcd-cluster-0 \
      --initial-cluster ${ETCD_NAME}=https://${LOCAL_IP}:2380 \
      --initial-cluster-state new \
      --data-dir=/var/lib/etcd

  sleep 1 # Wait for ETCD to start up.
fi

docker run --name etcdctl \
  --rm  \
  -v "${SECRETS_VOLUME_NAME}:/secrets" \
  -u 0 \
  --env=ETCDCTL_API=3 \
  --entrypoint "" \
  $ETCD_IMG etcdctl \
    --cacert=/secrets/ca.pem \
    --cert=/secrets/kubernetes.pem \
    --key=/secrets/kubernetes-key.pem \
    --endpoints=https://${INTERNAL_IP}:2379 \
    endpoint status -w table

if docker inspect $KUBE_APISERVER_CONTAINER_NAME > /dev/null 2>&1; then
  echo "Container ${KUBE_APISERVER_CONTAINER_NAME} already exists."
else
  docker run --name $KUBE_APISERVER_CONTAINER_NAME \
    -d --restart=unless-stopped \
    -p 6443:6443 \
    -v ${SECRETS_VOLUME_NAME}:/secrets \
    --entrypoint "" \
    $KUBE_APISERVER_IMG \
    kube-apiserver \
      --advertise-address=${INTERNAL_IP} \
      --allow-privileged=true \
      --apiserver-count=3 \
      --audit-log-maxage=30 \
      --audit-log-maxbackup=3 \
      --audit-log-maxsize=100 \
      --audit-log-path=/var/log/audit.log \
      --authorization-mode=Node,RBAC \
      --bind-address=0.0.0.0 \
      --client-ca-file=/secrets/ca.pem \
      --enable-admission-plugins=NamespaceLifecycle,NodeRestriction,LimitRanger,ServiceAccount,DefaultStorageClass,ResourceQuota \
      --etcd-cafile=/secrets/ca.pem \
      --etcd-certfile=/secrets/kubernetes.pem \
      --etcd-keyfile=/secrets/kubernetes-key.pem \
      --etcd-servers=https://${INTERNAL_IP}:2379 \
      --event-ttl=1h \
      --kubelet-certificate-authority=/secrets/ca.pem \
      --kubelet-client-certificate=/secrets/kubernetes.pem \
      --kubelet-client-key=/secrets/kubernetes-key.pem \
      --runtime-config='api/all=true' \
      --service-account-key-file=/secrets/service-account.pem \
      --service-account-signing-key-file=/secrets/service-account-key.pem \
      --service-account-issuer=https://${KUBERNETES_PUBLIC_ADDRESS}:6443 \
      --service-cluster-ip-range=10.32.0.0/24 \
      --service-node-port-range=30000-32767 \
      --tls-cert-file=/secrets/kubernetes.pem \
      --tls-private-key-file=/secrets/kubernetes-key.pem \
      --v=2

  sleep 5 # Wait for kube0-apiserver to start up.
fi

docker run --name kubectl \
  --rm  \
  -v "${SECRETS_VOLUME_NAME}:/secrets" \
  -u 0 \
  $KUBECTL_IMG cluster-info \
    --kubeconfig=/secrets/admin.kubeconfig \
    --server=https://${INTERNAL_IP}:6443

if docker inspect $KUBE_CONTROLLER_MANAGER_CONTAINER_NAME > /dev/null 2>&1; then
  echo "Container ${KUBE_CONTROLLER_MANAGER_CONTAINER_NAME} already exists."
else
  docker run --name $KUBE_CONTROLLER_MANAGER_CONTAINER_NAME \
    -d --restart=unless-stopped \
    -v ${SECRETS_VOLUME_NAME}:/secrets \
    --entrypoint "" \
    $KUBE_CONTROLLER_MANAGER_IMG \
    kube-controller-manager \
      --bind-address=0.0.0.0 \
      --cluster-cidr=10.200.0.0/16 \
      --cluster-name=kubernetes \
      --cluster-signing-cert-file=/secrets/ca.pem \
      --cluster-signing-key-file=/secrets/ca-key.pem \
      --kubeconfig=/secrets/kube-controller-manager.kubeconfig \
      --leader-elect=true \
      --root-ca-file=/secrets/ca.pem \
      --service-account-private-key-file=/secrets/service-account-key.pem \
      --service-cluster-ip-range=10.32.0.0/24 \
      --use-service-account-credentials=true \
      --v=2
fi

if docker inspect $KUBE_SCHEDULER_CONTAINER_NAME > /dev/null 2>&1; then
  echo "Container ${KUBE_SCHEDULER_CONTAINER_NAME} already exists."
else
  SOURCE_PATH=$(readlink -f "${BASH_SOURCE:-$0}")
  DIRNAME=$(dirname "$SOURCE_PATH")

  docker volume create $KUBE_SCHEDULER_VOLUME_NAME

  docker run --name busybox \
    --rm \
    -v "${DIRNAME}/configs:/configs-src" \
    -v "${KUBE_SCHEDULER_VOLUME_NAME}:/configs-dst" \
    busybox \
    cp /configs-src/kube-scheduler.yaml /configs-dst

  docker run --name $KUBE_SCHEDULER_CONTAINER_NAME \
    -d --restart=unless-stopped \
    -v ${SECRETS_VOLUME_NAME}:/secrets \
    -v ${KUBE_SCHEDULER_VOLUME_NAME}:/configs \
    --entrypoint "" \
    $KUBE_SCHEDULER_IMG \
    kube-scheduler \
      --config=/configs/kube-scheduler.yaml \
      --v=2
fi