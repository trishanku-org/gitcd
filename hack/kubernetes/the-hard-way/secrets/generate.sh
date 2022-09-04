#!/bin/bash

VOLUME_NAME="kubernetes-the-hard-way-secrets"
CFSSL="cfssl/cfssl:1.6.1"
SOURCE_PATH=$(readlink -f "${BASH_SOURCE:-$0}")
DIRNAME=$(dirname "$SOURCE_PATH")

docker volume create $VOLUME_NAME

# Generate CA cert
docker run --name cfssl \
  -i --rm \
  -v "${DIRNAME}/configs:/configs" \
  $CFSSL gencert -initca /configs/ca-csr.json | \
  docker run --name cfssljson \
    -i --rm \
    -v "${VOLUME_NAME}:/secrets" \
    -w /secrets \
    --entrypoint "" \
    $CFSSL cfssljson -bare ca

# Generate control-plane certs
for cert_name in admin kube-controller-manager kube-proxy kube-scheduler service-account; do
  docker run --name cfssl \
    -i --rm \
    -v "${DIRNAME}/configs:/configs" \
    -v "${VOLUME_NAME}:/secrets" \
    $CFSSL gencert \
      -ca=/secrets/ca.pem \
      -ca-key=/secrets/ca-key.pem \
      -config=/configs/ca-config.json \
      -profile=kubernetes \
      /configs/${cert_name}-csr.json | \
        docker run --name cfssljson \
          -i --rm \
          -v "${VOLUME_NAME}:/secrets" \
          -w /secrets \
          --entrypoint "" \
          $CFSSL cfssljson -bare ${cert_name}
done

# Generate kube-apiserver cert
if [ "${KUBERNETES_HOSTNAMES}" == "" ]; then
  KUBERNETES_HOSTNAMES="$(ifconfig | awk '/inet / {split($2,var,"/*"); print var[1]}' | tr '\n' ',')localhost,kubernetes,kubernetes.default,kubernetes.default.svc,kubernetes.default.svc.cluster,kubernetes.svc.cluster.local"
fi

echo "Using KUBERNETES_HOSTNAMES=${KUBERNETES_HOSTNAMES}"

docker run --name cfssl \
  -i --rm \
  -v "${DIRNAME}/configs:/configs" \
  -v "${VOLUME_NAME}:/secrets" \
  $CFSSL gencert \
    -ca=/secrets/ca.pem \
    -ca-key=/secrets/ca-key.pem \
    -config=/configs/ca-config.json \
    -hostname=${KUBERNETES_HOSTNAMES} \
    -profile=kubernetes \
    /configs/kubernetes-csr.json | \
      docker run --name cfssljson \
        -i --rm \
        -v "${VOLUME_NAME}:/secrets" \
        -w /secrets \
        --entrypoint "" \
        $CFSSL cfssljson -bare kubernetes

# Generate worker-0 cert
if [ "${WORKER_HOSTNAMES}" == "" ]; then
  WORKER_HOSTNAMES="$(ifconfig | awk '/inet / {split($2,var,"/*"); print var[1]}' | tr '\n' ',')localhost,worker-0"
fi

echo "Using WORKER_HOSTNAMES=${WORKER_HOSTNAMES}"

docker run  --name cfssl \
  -i --rm \
  -v "${DIRNAME}/configs:/configs" \
  -v "${VOLUME_NAME}:/secrets" \
  $CFSSL gencert \
    -ca=/secrets/ca.pem \
    -ca-key=/secrets/ca-key.pem \
    -config=/configs/ca-config.json \
    -hostname=${WORKER_HOSTNAMES} \
    -profile=kubernetes \
    /configs/worker-0-csr.json | \
      docker run --name cfssljson \
        -i --rm \
        -v "${VOLUME_NAME}:/secrets" \
        -w /secrets \
        --entrypoint "" \
        $CFSSL cfssljson -bare worker-0

#Generate kubeconfigs
KUBECTL=bitnami/kubectl:1.21

function generate_kubeconfig {
  local component="$1"
  local user="$2"
  local url="$3"

  echo "Building kubeconfig for ${component}, with user ${user} and URL ${url}"

  docker run --name kubectl \
    --rm \
    -v "${VOLUME_NAME}:/secrets" \
    -u 0 \
    $KUBECTL config set-cluster kubernetes-the-hard-way \
      --certificate-authority=/secrets/ca.pem \
      --embed-certs=true \
      "--server=${url}" \
      --kubeconfig=/secrets/${component}.kubeconfig

  docker run --name kubectl \
    --rm \
    -v "${VOLUME_NAME}:/secrets" \
    -u 0 \
    $KUBECTL config set-credentials ${user} \
      --client-certificate=/secrets/${component}.pem \
      --client-key=/secrets/${component}-key.pem \
      --embed-certs=true \
      --kubeconfig=/secrets/${component}.kubeconfig

  docker run --name kubectl \
    --rm \
    -v "${VOLUME_NAME}:/secrets" \
    -u 0 \
    $KUBECTL config set-context default \
      --cluster=kubernetes-the-hard-way \
      --user=${user} \
      --kubeconfig=/secrets/${component}.kubeconfig

  docker run --name kubectl \
    --rm \
    -v "${VOLUME_NAME}:/secrets" \
    -u 0 \
    $KUBECTL config use-context default --kubeconfig=/secrets/${component}.kubeconfig
}

if [ "$INTERNAL_SERVER_URL" == "" ]; then
  INTERNAL_SERVER_URL="https://127.0.0.1:6443"
fi

if [ "$EXTERNAL_SERVER_URL" == "" ]; then
  EXTERNAL_SERVER_URL="https://$(ifconfig | awk '/inet / {split($2,var,"/*"); print var[1]}' | grep -v 127.0.0.1 | head -n 1):6443"
fi

echo "Using INTERNAL_SERVER_URL=${INTERNAL_SERVER_URL} and EXTERNAL_SERVER_URL=${EXTERNAL_SERVER_URL}"

generate_kubeconfig kube-controller-manager system:kube-controller-manager "$INTERNAL_SERVER_URL"
generate_kubeconfig kube-scheduler system:kube-scheduler "$INTERNAL_SERVER_URL"
generate_kubeconfig admin admin "$INTERNAL_SERVER_URL"
generate_kubeconfig kube-proxy system:kube-proxy "$EXTERNAL_SERVER_URL"
generate_kubeconfig worker-0 system:node:worker-0 "$EXTERNAL_SERVER_URL"
