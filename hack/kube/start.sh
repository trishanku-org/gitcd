#!/bin/bash

set -x

function start_gitcd {
    docker volume create gitcd-backend
    echo 'git init -b main && git config user.email "trishanku@heaven.com" && git config user.name trishanku' | \
        docker run -i --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 sh
    echo 'touch init && git add init && git commit -m init' | \
        docker run -i --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 sh
    docker run --rm -v gitcd-backend:/backend trishanku/gitcd:latest init --repo=/backend
    docker run --name gitcd -d -v gitcd-backend:/backend -p 2379:2379 trishanku/gitcd:latest serve --repo=/backend --debug
    docker run --rm --network=container:gitcd --entrypoint etcdctl bitnami/etcd:3 --insecure-transport endpoint status
}

function prepare_certs {
    docker volume create kube-certs

    docker run -i --rm -v kube-certs:/tls -w /tls nginx:1 sh <<EOF
set -x

curl -L https://storage.googleapis.com/kubernetes-release/easy-rsa/easy-rsa.tar.gz | tar -xzv

cd easy-rsa-master/easyrsa3

./easyrsa init-pki

./easyrsa --batch "--req-cn=ca@trishanku.heaven.com" build-ca nopass

./easyrsa --subject-alt-name="IP:192.168.1.16,\
DNS:localhost,\
DNS:kubernetes,\
DNS:kubernetes.default,\
DNS:kubernetes.default.svc,\
DNS:kubernetes.default.svc.cluster,\
DNS:kubernetes.default.svc.cluster.local" \
    --days=10000 \
    build-server-full kubernetes nopass

cd /tls
ln -s /tls/easy-rsa-master/easyrsa3/pki/ca.crt ca.crt
ln -s /tls/easy-rsa-master/easyrsa3/pki/issued/kubernetes.crt tls.crt
ln -s /tls/easy-rsa-master/easyrsa3/pki/private/kubernetes.key tls.key
EOF

}

function start_kube_apiserver {
    docker run --name kube-apiserver -d -v kube-certs:/tls --network=container:gitcd \
        --entrypoint kube-apiserver \
        k8s.gcr.io/kube-apiserver:v1.23.2 \
            --client-ca-file=/tls/ca.crt \
            --etcd-compaction-interval=0 \
            --etcd-servers=http://localhost:2379 \
            --secure-port=6443 \
            --service-account-issuer=https://kube-apiserver/ \
            --service-account-key-file=/tls/tls.key \
            --service-account-signing-key-file=/tls/tls.key \
            --storage-media-type=application/yaml \
            --tls-cert-file=/tls/tls.crt \
            --tls-private-key-file=/tls/tls.key \
            --watch-cache=false
}

function kind_create_cluster {
    kind create cluster --config ./hack/kube/kind-config.yaml
}

start_gitcd
# kind_create_cluster
