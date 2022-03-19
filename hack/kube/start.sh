#!/bin/bash

set -x

function start_gitcd_container {
    local container_name=$1
    local port=$2

    docker volume create "$container_name"

    echo 'git init -b main && git config user.email "trishanku@heaven.com" && git config user.name trishanku' | \
        docker run -i --rm -v "${container_name}:/backend" -w /backend bitnami/git:2 sh
    echo 'touch init && git add init && git commit -m init' | \
        docker run -i --rm -v "${container_name}:/backend" -w /backend bitnami/git:2 sh

    docker run --rm -v "${container_name}:/backend" trishanku/gitcd:latest init --repo=/backend

    docker run --name "$container_name" \
        -d -v "${container_name}:/backend" \
        -p "${port}:2379" \
        trishanku/gitcd:latest \
        serve --repo=/backend --debug

    docker run --rm "--network=container:${container_name}" --entrypoint etcdctl bitnami/etcd:3 --insecure-transport endpoint status
}

function start_etcd_container {
    local container_name=$1
    local port=$2

    docker volume create "$container_name"
    
    docker run --name "$container_name" \
        -d -v "${container_name}:/backend" \
        -p "${port}:2379" \
        --env ETCD_DATA_DIR=/backend/default.etcd \
        --env ALLOW_NONE_AUTHENTICATION=yes \
        bitnami/etcd:3 

    docker run --rm "--network=container:${container_name}" --entrypoint etcdctl bitnami/etcd:3 --insecure-transport endpoint status
}

function start_gitcd {
    local base_dir=$1
    local port=$2
    local repo_dir="${base_dir}/repo"
    local BACKEND_VERSION=$(cat VERSION)

    mkdir -p "$repo_dir"
    
    cd "$repo_dir"

    git init -b main && \
        touch init && \
        git add init && \
        git commit -m init
    
    cd -

    go run \
	    -ldflags "-X github.com/trishanku/gitcd/pkg/backend.Version=${BACKEND_VERSION}" \
	    main.go init "--repo=${repo_dir}"

    go run \
        -ldflags "-X github.com/trishanku/gitcd/pkg/backend.Version=${BACKEND_VERSION}" \
		main.go serve \
            "--repo=${repo_dir}" \
            "--listen-urls=default=http://0.0.0.0:${port}" \
            "--advertise-client-urls=default=http://localhost:${port}" > "${base_dir}/gitcd.log" 2>&1 &

    ETCDCTL_API=3 etcdctl "--endpoints=http://localhost:${port}" --insecure-transport endpoint status
}

function start_etcd {
    local base_dir=$1
    local data_dir="${base_dir}/default.etcd"

    mkdir -p "$data_dir"

    etcd "--data-dir=${data_dir}" \
        --listen-client-urls=http://0.0.0.0:2379 \
        --advertise-client-urls=http://localhost:2379 > "${base_dir}/etcd.log" 2>&1 &

    ETCDCTL_API=3 etcdctl --insecure-transport endpoint status
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
            --etcd-servers=http://192.168.1.6:2479 \
            --etcd-servers-overwrites=http://localhost:2379 \
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
    kind create cluster --config ./hack/kube/kind-config.yaml --verbosity=2 --retain
}

# start_etcd /tmp/trishanku/etcd-events
# start_gitcd /tmp/trishanku/gitcd-main 2479
# start_gitcd /tmp/trishanku/gitcd-nodes 2579
# start_gitcd /tmp/trishanku/gitcd-leases 2679
start_etcd_container etcd-events 2379
start_gitcd_container gitcd-main 2479
start_gitcd_container gitcd-nodes 2579
start_gitcd_container gitcd-leases 2679
start_gitcd_container gitcd-priorityclasses 2779
start_gitcd_container gitcd-pods 2879
start_gitcd_container gitcd-configmaps 2979
kind_create_cluster
