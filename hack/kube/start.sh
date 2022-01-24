#!/bin/bash

set -x

function start_gitcd {
    docker volume create gitcd-backend
    echo 'git init -b main && git config user.email "trishanku@heaven.com" && git config user.name trishanku' | \
        docker run -i --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 sh
    echo 'touch init && git add init && git commit -m init' | \
        docker run -i --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 sh
    docker run --rm -v gitcd-backend:/backend trishanku/gitcd:latest init --repo=/backend
    docker run --name gitcd -d -v gitcd-backend:/backend trishanku/gitcd:latest serve --repo=/backend --debug
    docker run --rm --network=container:gitcd --entrypoint etcdctl bitnami/etcd:3 --insecure-transport endpoint status
}

function prepare_certs {
    docker volume create kube-certs

    docker run -i --rm -v kube-certs:/tls -w /tls busybox:1 dd of=tls.conf <<EOF
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no

[req_distinguished_name]
C = IN
ST = KA
L = Bengaluru
O = Trishanku
OU = Gitcd
CN = gitcd

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = gitcd
DNS.3 = gitcd.trishanku
DNS.4 = gitcd.trishanku.svc
DNS.5 = kube-apiserver
DNS.6 = kube-apiserver.trishanku
DNS.7 = kube-apiserver.trishanku.svc

EOF

    docker run --rm -v kube-certs:/tls -w /tls --entrypoint openssl nginx:1 req -newkey rsa:4096 -x509 -sha256 -days 365 \
        -nodes -extensions v3_req \
		-config tls.conf \
		-out tls.crt \
        -keyout tls.key

    docker run --rm -v kube-certs:/tls -w /tls busybox:1 ln -s tls.crt ca.crt
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

start_gitcd
prepare_certs
start_kube_apiserver
