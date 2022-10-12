#!/bin/bash

SECRETS_VOLUME_NAME="kubernetes-the-hard-way-secrets"
REPO_THE_REST="https://github.com/amshuman-kr/trishanku-the-hard-way-the-rest"
REPO_NODES="https://github.com/amshuman-kr/trishanku-the-hard-way-nodes"
REPO_LEASES="https://github.com/amshuman-kr/trishanku-the-hard-way-leases"
REPO_PRIORITYCLASSES="https://github.com/amshuman-kr/trishanku-the-hard-way-priorityclasses"
REPO_PODS="https://github.com/amshuman-kr/trishanku-the-hard-way-pods"
REPO_CONFIGMAPS="https://github.com/amshuman-kr/trishanku-the-hard-way-configmaps"

LABEL="trishanku=the-hard-way"
GIT_IMG=bitnami/git:2
GITCD_IMG=trishanku/gitcd:latest
ETCD_IMG=bitnami/etcd:3
BLANK_BRANCH_PREFIX="blank"

KUBE_VERSION=v1.24.4
KUBECTL_IMG=bitnami/kubectl:1.24.4
KUBE_IMG_BASE=registry.k8s.io
KUBE_APISERVER_IMG=${KUBE_IMG_BASE}/kube-apiserver:${KUBE_VERSION}
KUBE_CONTROLLER_MANAGER_IMG=${KUBE_IMG_BASE}/kube-controller-manager:${KUBE_VERSION}
KUBE_SCHEDULER_IMG=${KUBE_IMG_BASE}/kube-scheduler:${KUBE_VERSION}
KUBE_APISERVER_CONTAINER_NAME=kube-apiserver
KUBE_CONTROLLER_MANAGER_CONTAINER_NAME=kube-controller-manager
KUBE_SCHEDULER_CONTAINER_NAME=kube-scheduler
KUBE_SCHEDULER_VOLUME_NAME="kubernetes-the-hard-way-scheduler"

BUSYBOX_IMG=busybox:1

function store_repo_credentials {
  local USER=
  local PASSWORD=

  read -p "User for the repo ${REPO_THE_REST}: " USER
  read -sp "Password for the repo ${REPO_THE_REST}: " PASSWORD
  echo

  cat <<EOF | \
    docker run --name "git-credentials-helper-store" \
      -i --rm \
      --label "$LABEL" \
      -v "${SECRETS_VOLUME_NAME}:/secrets" \
      $GIT_IMG bash
mkdir -p "/secrets"
for file in /secrets/.gitconfig /secrets/.git-credentials; do
  touch \$file && ln -s \$file ~/\$(basename \$file)
done

chmod 600 /secrets/.git-credentials

git config --global credential.helper store

cat <<CRED_SPEC_EOF | git credential approve
url=https://github.com
username=$USER
password=$PASSWORD
CRED_SPEC_EOF

# echo url=https://github.com | git credential fill
EOF
}

function volume_exists {
  local VOLUME="$1"

  docker volume inspect "$VOLUME" > /dev/null 2>&1
}

function create_volume_if_not_exists {
  local VOLUME="$1"

  volume_exists "$VOLUME" || docker volume create --label "$LABEL" "$VOLUME"
}

function container_exists {
  local CONTAINER="$1"

  docker container inspect "$CONTAINER" > /dev/null 2>&1 
}

function start_etcd {
  local CONTAINER="$1"
  local CLUSTER_TOKEN="$2"
  local DOCKER_RUN_ARGS="$3"
  local VOLUME="$CONTAINER"

  create_volume_if_not_exists "$VOLUME"
  if container_exists "$CONTAINER"; then
    docker container restart "$CONTAINER"
  else
    docker run --name "$CONTAINER" \
      -d --restart=unless-stopped \
      --label "$LABEL" \
      $DOCKER_RUN_ARGS \
      -v "${VOLUME}:/var/lib/etcd" \
      -u 0 \
      --entrypoint "" \
      $ETCD_IMG etcd \
        --initial-cluster-token "$CLUSTER_TOKEN" \
        --data-dir=/var/lib/etcd
  fi
}

function git_clone_and_prepare_branch {
  local CONTAINER="$1"
  local REMOTE_REPO="$2"
  local BRANCH_PREFIX="$3"
  local REMOTE_BRANCH_PREFIX="$4"
  local VOLUME="$CONTAINER"

  create_volume_if_not_exists "$VOLUME"
  if container_exists "$CONTAINER"; then
    docker container restart "$CONTAINER"
  else
    cat <<EOF | \
      docker run --name "$CONTAINER" \
        -i --rm \
        --label "$LABEL" \
        -v "${VOLUME}:/backend" \
        -v "${SECRETS_VOLUME_NAME}:/secrets" \
        $GIT_IMG bash
# set -x

for file in /secrets/.gitconfig /secrets/.git-credentials; do
  ln -s \$file ~/\$(basename \$file)
done

git clone --bare --branch "${REMOTE_BRANCH_PREFIX}/data" "$REMOTE_REPO" /backend

cd /backend

git config core.logAllRefUpdates always
git config --unset-all remote.origin.fetch
git config --unset-all remote.origin.push

function prepare_branch {
  local BRANCH="\$1"
  local REMOTE_BRANCH="\$2"

  if  git show-branch "\$BRANCH" > /dev/null 2>&1; then
    echo "Branch \${BRANCH} already exists."
  else
    git branch "\$BRANCH" "\${REMOTE_BRANCH}" --no-track
  fi

  git config --add remote.origin.fetch "refs/heads/\${REMOTE_BRANCH}:refs/heads/\${REMOTE_BRANCH}"
  git config --add remote.origin.push "refs/heads/\${BRANCH}:refs/heads/\${BRANCH}"
}

prepare_branch "${BRANCH_PREFIX}/data" "${REMOTE_BRANCH_PREFIX}/data"
prepare_branch "${BRANCH_PREFIX}/metadata" "${REMOTE_BRANCH_PREFIX}/metadata"

# Ensure that if branches were created they are also created in remote.
git push
EOF
  fi
}

function start_git_merge_if_required {
  local CONTAINER_PREFIX="$1"
  local REMOTE_REPO="$2"
  local BRANCH_PREFIX="$3"
  local REMOTE_BRANCH_PREFIX="$4"
  local MERGE_RETENTION_POLICIES="$5"
  local MERGE_CONFLICT_RESOLUTIONS="$6"
  local CONTAINER="${CONTAINER_PREFIX}git-merge-to-${BRANCH_PREFIX}-from-${REMOTE_BRANCH_PREFIX}"
  local VOLUME="${CONTAINER}"
  local DATA_BRANCH="${BRANCH_PREFIX}/data"
  local METADATA_BRANCH="${BRANCH_PREFIX}/metadata"
  local DATA_REF="refs/heads/${DATA_BRANCH}"
  local METADATA_REF="refs/heads/${METADATA_BRANCH}"
  local REMOTE="origin"
  local REMOTE_DATA_BRANCH="${REMOTE_BRANCH_PREFIX}/data"
  local REMOTE_METADATA_BRANCH="${REMOTE_BRANCH_PREFIX}/metadata"
  local REMOTE_DATA_REF="refs/heads/${REMOTE_DATA_BRANCH}"
  local REMOTE_METADATA_REF="refs/heads/${REMOTE_METADATA_BRANCH}"
  local ENTRYPOINT_VOLUME="${CONTAINER}-entrypoint"

  if [ "$BRANCH_PREFIX" == "$REMOTE_BRANCH_PREFIX" ]; then
    return 0
  fi

  if container_exists "$CONTAINER"; then
    docker container restart "$CONTAINER"
  else
    git_clone_and_prepare_branch "$CONTAINER" "$REMOTE_REPO" "$BRANCH_PREFIX" "$REMOTE_BRANCH_PREFIX"

    create_volume_if_not_exists "$ENTRYPOINT_VOLUME"
    cat <<EOF | \
      docker run --name "$CONTAINER" \
        -i --rm \
        --label "$LABEL" \
        -v "${ENTRYPOINT_VOLUME}:/entrypoint" \
        $BUSYBOX_IMG sh
cat <<INNER_EOF > /entrypoint/entrypoint.sh
#!/bin/sh

for file in /secrets/.gitconfig /secrets/.git-credentials; do
  target="\\\${HOME}/\\\$(basename \\\$file)"
  test -L \\\$target || ln -s \\\$file \\\$target
done

exec /gitcd pull \
  --repo=/backend \
  --committer-name=${BRANCH_PREFIX}-from-${REMOTE_BRANCH_PREFIX} \
  --data-reference-names=default=${DATA_REF} \
  --metadata-reference-names=default=${METADATA_REF} \
  --push-after-merges=default=true \
  ${MERGE_RETENTION_POLICIES} \
  --merge-conflict-resolutions=${MERGE_CONFLICT_RESOLUTIONS} \
  --remote-names=default=${REMOTE} \
  --remote-data-reference-names=default=${REMOTE_DATA_REF} \
  --remote-meta-reference-names=default=${REMOTE_METADATA_REF} \
  --no-fast-forwards=default=false \
  --pull-ticker-duration=11s
INNER_EOF

chmod +x /entrypoint/entrypoint.sh
EOF

    docker run --name "$CONTAINER" \
      -d --restart=unless-stopped \
      --label "$LABEL" \
      ${NETWORK_ARGS} \
      -v "${VOLUME}:/backend" \
      -v "${SECRETS_VOLUME_NAME}:/secrets" \
      -v "${ENTRYPOINT_VOLUME}:/entrypoint" \
      --entrypoint= \
      $GITCD_IMG /entrypoint/entrypoint.sh
  fi
}

function start_gitcd {
  local PORT="$1"
  local CONTAINER_PREFIX="$2"
  local REMOTE_REPO="$3"
  local BRANCH_PREFIX="$4"
  local REMOTE_BRANCH_PREFIX="$5"
  local MERGE_RETENTION_POLICIES="$6"
  local MERGE_CONFLICT_RESOLUTIONS="$7"
  local MERGE_CONFLICT_RESOLUTIONS_MERGE="$8"
  local NETWORK_ARGS="$9"
  local CONTAINER="${CONTAINER_PREFIX}${BRANCH_PREFIX}"
  local VOLUME="${CONTAINER}"
  local DATA_BRANCH="${BRANCH_PREFIX}/data"
  local METADATA_BRANCH="${BRANCH_PREFIX}/metadata"
  local DATA_REF="refs/heads/${DATA_BRANCH}"
  local METADATA_REF="refs/heads/${METADATA_BRANCH}"
  local REMOTE="origin"
  local REMOTE_DATA_BRANCH="${REMOTE_BRANCH_PREFIX}/data"
  local REMOTE_METADATA_BRANCH="${REMOTE_BRANCH_PREFIX}/metadata"
  local REMOTE_DATA_REF="refs/heads/${REMOTE_DATA_BRANCH}"
  local REMOTE_METADATA_REF="refs/heads/${REMOTE_METADATA_BRANCH}"
  local ENTRYPOINT_VOLUME="${CONTAINER}-entrypoint"

  if container_exists "$CONTAINER"; then
    docker container restart "$CONTAINER"
  else
    git_clone_and_prepare_branch "$CONTAINER" "$REMOTE_REPO" "$BRANCH_PREFIX" "$REMOTE_BRANCH_PREFIX"

    create_volume_if_not_exists "$ENTRYPOINT_VOLUME"
    cat <<EOF | \
      docker run --name "$CONTAINER" \
        -i --rm \
        --label "$LABEL" \
        -v "${ENTRYPOINT_VOLUME}:/entrypoint" \
        $BUSYBOX_IMG sh
cat <<INNER_EOF > /entrypoint/entrypoint.sh
#!/bin/sh

for file in /secrets/.gitconfig /secrets/.git-credentials; do
  target="\\\${HOME}/\\\$(basename \\\$file)"
  test -L \\\$target || ln -s \\\$file \\\$target
done

exec /gitcd serve \
  --repo=/backend \
  --committer-name=${BRANCH_PREFIX} \
  --data-reference-names=default=${DATA_REF} \
  --metadata-reference-names=default=${METADATA_REF} \
  --key-prefixes=default=/registry \
  --pull-ticker-duration=20s \
  --push-after-merges=default=true \
  ${MERGE_RETENTION_POLICIES} \
  --merge-conflict-resolutions=${MERGE_CONFLICT_RESOLUTIONS} \
  --remote-names=default=${REMOTE} \
  --no-fast-forwards=default=false \
  --remote-data-reference-names=default=${REMOTE_DATA_REF} \
  --remote-meta-reference-names=default=${REMOTE_METADATA_REF} \
  --listen-urls=default=http://0.0.0.0:${PORT}/ \
  --advertise-client-urls=default=http://127.0.0.1:${PORT}/ \
  --watch-dispatch-channel-size=1
INNER_EOF

chmod +x /entrypoint/entrypoint.sh
EOF

    docker run --name "$CONTAINER" \
      -d --restart=unless-stopped \
      --label "$LABEL" \
      ${NETWORK_ARGS} \
      -v "${VOLUME}:/backend" \
      -v "${SECRETS_VOLUME_NAME}:/secrets" \
      -v "${ENTRYPOINT_VOLUME}:/entrypoint" \
      --entrypoint= \
      $GITCD_IMG /entrypoint/entrypoint.sh
  
    # Separate container to merge to upstream.
    start_git_merge_if_required "$CONTAINER_PREFIX" "$REMOTE_REPO" \
      "$REMOTE_BRANCH_PREFIX" "$BRANCH_PREFIX" "$MERGE_RETENTION_POLICIES" "$MERGE_CONFLICT_RESOLUTIONS_MERGE"
  fi
}

function start_apiserver {
  local CONTAINER_PREFIX="$1"
  local BRANCH_PREFIX="$2"
  local REMOTE_BRANCH_PREFIX="$3"
  local MERGE_RETENTION_POLICIES="$4"
  local MERGE_CONFLICT_RESOLUTIONS="$5"
  local MERGE_CONFLICT_RESOLUTIONS_MERGE="$6"
  local DOCKER_RUN_ARGS="$7"
  local ETCD_EVENTS_CONTAINER="${CONTAINER_PREFIX}etcd-events-${BRANCH_PREFIX}"
  local GITCD_CONTAINER_PREFIX="$CONTAINER_PREFIX"
  local NETWORK_ARGS="--network=container:${ETCD_EVENTS_CONTAINER}"
  local APISERVER_CONTAINER="${CONTAINER_PREFIX}kube-apiserver-${BRANCH_PREFIX}"

  start_etcd "$ETCD_EVENTS_CONTAINER" "$BRANCH_PREFIX" "$DOCKER_RUN_ARGS"
  start_gitcd "2479" "${GITCD_CONTAINER_PREFIX}the-rest-" "$REPO_THE_REST" "$BRANCH_PREFIX" "$REMOTE_BRANCH_PREFIX" \
    "$MERGE_RETENTION_POLICIES" "$MERGE_CONFLICT_RESOLUTIONS" "$MERGE_CONFLICT_RESOLUTIONS_MERGE" "$NETWORK_ARGS"

  if container_exists "$APISERVER_CONTAINER"; then
    docker container restart "$APISERVER_CONTAINER"
  else
    if [ "$INTERNAL_IP" == "" ]; then
      INTERNAL_IP="$(ifconfig | awk '/inet / {split($2,var,"/*"); print var[1]}' | grep -v 127.0.0.1 | head -n 1)"
    fi

    if  [ "$KUBERNETES_PUBLIC_ADDRESS" == "" ]; then
      KUBERNETES_PUBLIC_ADDRESS="$INTERNAL_IP"
    fi

    docker run --name "$APISERVER_CONTAINER" \
      -d --restart=no \
      --label "$LABEL" \
      "$NETWORK_ARGS" \
      -v ${SECRETS_VOLUME_NAME}:/secrets \
      --entrypoint "" \
      $KUBE_APISERVER_IMG \
      kube-apiserver \
        --advertise-address=${INTERNAL_IP} \
        --allow-privileged=true \
        --apiserver-count=1 \
        --audit-log-maxage=30 \
        --audit-log-maxbackup=3 \
        --audit-log-maxsize=100 \
        --audit-log-path=/var/log/audit.log \
        --authorization-mode=Node,RBAC \
        --bind-address=0.0.0.0 \
        --client-ca-file=/secrets/ca.pem \
        --enable-admission-plugins=NamespaceLifecycle,NodeRestriction,LimitRanger,ServiceAccount,DefaultStorageClass,ResourceQuota \
        --enable-garbage-collector=false \
        --etcd-compaction-interval=0 \
        --etcd-count-metric-poll-period=0 \
        --etcd-db-metric-poll-interval=0 \
        --etcd-healthcheck-timeout=10s \
        --etcd-servers=http://127.0.0.1:2479 \
        --etcd-servers-overrides=/events#http://127.0.0.1:2379 \
        --event-ttl=1h \
        --kubelet-certificate-authority=/secrets/ca.pem \
        --kubelet-client-certificate=/secrets/kubernetes.pem \
        --kubelet-client-key=/secrets/kubernetes-key.pem \
        --lease-reuse-duration-seconds=120 \
        --service-account-key-file=/secrets/service-account.pem \
        --service-account-signing-key-file=/secrets/service-account-key.pem \
        --service-account-issuer=https://${KUBERNETES_PUBLIC_ADDRESS}:6443 \
        --service-cluster-ip-range=10.32.0.0/24 \
        --service-node-port-range=30000-32767 \
        --storage-media-type=application/yaml \
        --tls-cert-file=/secrets/kubernetes.pem \
        --tls-private-key-file=/secrets/kubernetes-key.pem \
        --v=2 \
        --watch-cache=false

    echo -n "Waiting 10s for kube-apiserver to start up..." && sleep 10 && echo

    docker run --name kubectl \
      --rm  \
      --label "$LABEL" \
      "$NETWORK_ARGS" \
      -v "${SECRETS_VOLUME_NAME}:/secrets" \
      -u 0 \
      $KUBECTL_IMG cluster-info \
        --kubeconfig=/secrets/admin.kubeconfig

    echo -n "Waiting 10s for gitcd to pull and push changes ones up..." && sleep 10 && echo
  fi
}

function start_kube_controller_manager {
  local CONTAINER_PREFIX="$1"
  local BRANCH_PREFIX="$2"
  local DOCKER_RUN_ARGS="$3"
  local CONTAINER="${CONTAINER_PREFIX}kube-controller-manager-${BRANCH_PREFIX}"

  if container_exists "$CONTAINER"; then
    docker container restart "$CONTAINER"
  else
    docker run --name $CONTAINER \
      -d --restart=unless-stopped \
      --label "$LABEL" \
      $DOCKER_RUN_ARGS \
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
        --leader-elect=false \
        --root-ca-file=/secrets/ca.pem \
        --service-account-private-key-file=/secrets/service-account-key.pem \
        --service-cluster-ip-range=10.32.0.0/24 \
        --use-service-account-credentials=true \
        --concurrent-deployment-syncs=1 \
        --concurrent-endpoint-syncs=1 \
        --concurrent-service-endpoint-syncs=1 \
        --concurrent-gc-syncs=1 \
        --concurrent-namespace-syncs=1 \
        --concurrent-replicaset-syncs=1 \
        --concurrent-rc-syncs=1 \
        --concurrent-resource-quota-syncs=1 \
        --concurrent-service-syncs=1 \
        --concurrent-serviceaccount-token-syncs=1 \
        --concurrent-statefulset-syncs=1 \
        --concurrent-ttl-after-finished-syncs=1 \
        --v=2
  fi
}

function start_kube_controller_manager_with_gitcd {
  local CONTAINER_PREFIX="$1"
  local BRANCH_PREFIX="$2"
  local REMOTE_BRANCH_PREFIX="$3"
  local DOCKER_RUN_ARGS="$4"
  local ETCD_EVENTS_CONTAINER="${CONTAINER_PREFIX}etcd-events-${BRANCH_PREFIX}"

  start_apiserver "$CONTAINER_PREFIX" "$BRANCH_PREFIX" "$REMOTE_BRANCH_PREFIX" \
    "--merge-retention-policies-exclude=default=leases/.*,flowschemas/.*,/masterleases/.*" \
    "default=1" \
    "default=2" \
    "$DOCKER_RUN_ARGS"
  start_kube_controller_manager "$CONTAINER_PREFIX" "$BRANCH_PREFIX" "--network=container:${ETCD_EVENTS_CONTAINER}"
}

function start_kube_scheduler {
  local CONTAINER_PREFIX="$1"
  local BRANCH_PREFIX="$2"
  local DOCKER_RUN_ARGS="$3"
  local CONTAINER="${CONTAINER_PREFIX}kube-scheduler-${BRANCH_PREFIX}"
  local VOLUME="$CONTAINER"
  local SOURCE_PATH=$(readlink -f "${BASH_SOURCE:-$0}")
  local DIRNAME=$(dirname "$SOURCE_PATH")

  volume_exists "$VOLUME" || (docker volume create --label "$LABEL" "$VOLUME" && \
    docker run --name busybox \
      --rm \
      --label "$LABEL" \
      -v "${DIRNAME}/configs:/configs-src" \
      -v "${VOLUME}:/configs-dst" \
      $BUSYBOX_IMG \
        cp /configs-src/kube-scheduler.yaml /configs-dst)

  if container_exists "$CONTAINER"; then
    docker container restart "$CONTAINER"
  else
    docker run --name "$CONTAINER" \
      -d --restart=unless-stopped \
      --label "$LABEL" \
      $DOCKER_RUN_ARGS \
      -v ${SECRETS_VOLUME_NAME}:/secrets \
      -v ${VOLUME}:/configs \
      --entrypoint "" \
      $KUBE_SCHEDULER_IMG \
      kube-scheduler \
        --config=/configs/kube-scheduler.yaml \
        --v=4
  fi
}

function start_kube_scheduler_with_gitcd {
  local CONTAINER_PREFIX="$1"
  local BRANCH_PREFIX="$2"
  local REMOTE_BRANCH_PREFIX="$3"
  local MERGE_CONFLICT_RESOLUTIONS="$4"
  local DOCKER_RUN_ARGS="$5"
  local ETCD_EVENTS_CONTAINER="${CONTAINER_PREFIX}etcd-events-${BRANCH_PREFIX}"

  start_apiserver "$CONTAINER_PREFIX" "$BRANCH_PREFIX" "$REMOTE_BRANCH_PREFIX" \
    "--merge-retention-policies-exclude=default=leases/.*,flowschemas/.*,/masterleases/.*" \
    "default=1" \
    "default=2" \
  "$DOCKER_RUN_ARGS"
  start_kube_scheduler "$CONTAINER_PREFIX" "$BRANCH_PREFIX" "--network=container:${ETCD_EVENTS_CONTAINER}"
}

function start_kube_scheduler_with_gitcd {
  local CONTAINER_PREFIX="$1"
  local BRANCH_PREFIX="$2"
  local REMOTE_BRANCH_PREFIX="$3"
  local MERGE_CONFLICT_RESOLUTIONS="$4"
  local DOCKER_RUN_ARGS="$5"
  local ETCD_EVENTS_CONTAINER="${CONTAINER_PREFIX}etcd-events-${BRANCH_PREFIX}"

  start_apiserver "$CONTAINER_PREFIX" "$BRANCH_PREFIX" "$REMOTE_BRANCH_PREFIX" \
    "--merge-retention-policies-exclude=default=leases/.*,flowschemas/.*,/masterleases/.*" \
    "default=1" \
    "default=2" \
  "$DOCKER_RUN_ARGS"
  start_kube_scheduler "$CONTAINER_PREFIX" "$BRANCH_PREFIX" "--network=container:${ETCD_EVENTS_CONTAINER}"
}

function apply_kubelet_rbac {
  local DOCKER_RUN_ARGS="$1"
  local SOURCE_PATH=$(readlink -f "${BASH_SOURCE:-$0}")
  local DIRNAME=$(dirname "$SOURCE_PATH")

  cat "${DIRNAME}/configs/kubelet-rbac.yaml" | \
  docker run --name kubelet \
    -i --rm  \
    --label "$LABEL" \
    $DOCKER_RUN_ARGS \
    -v "${SECRETS_VOLUME_NAME}:/secrets" \
    -u 0 \
    $KUBECTL_IMG apply \
      -f - \
      --kubeconfig=/secrets/admin.kubeconfig
}

function start_kubelet_apiserver_with_gitcd {
  local CONTAINER_PREFIX="$1"
  local BRANCH_PREFIX="$2"
  local REMOTE_BRANCH_PREFIX="$3"
  local MERGE_CONFLICT_RESOLUTIONS="$4"
  local DOCKER_RUN_ARGS="$5"
  local ETCD_EVENTS_CONTAINER="${CONTAINER_PREFIX}etcd-events-${BRANCH_PREFIX}"

  start_apiserver "$CONTAINER_PREFIX" "$BRANCH_PREFIX" "$REMOTE_BRANCH_PREFIX" \
    "--merge-retention-policies-exclude=default=leases/.*,flowschemas/.*,/masterleases/.*" \
    "default=1" \
    "default=2" \
  "$DOCKER_RUN_ARGS"
}

# set -x

OPTION="$1"

case "$OPTION" in
repo-credentials)
  store_repo_credentials
  ;;

control-plane)
  start_kube_controller_manager_with_gitcd "trishanku-the-hard-way-" "kcm" "upstream" "-p 2379-2380 -p 6443:6443"
  start_kube_scheduler_with_gitcd "trishanku-the-hard-way-" "scheduler" "upstream" "-p 2379-2380"
  apply_kubelet_rbac "--network=container:trishanku-the-hard-way-etcd-events-kcm"
  ;;

kubelet-apiserver)
  start_kubelet_apiserver_with_gitcd "trishanku-the-hard-way-" "kubelet" "upstream" "-p 2379-2380 -p 6443:6443"
  ;;

esac
