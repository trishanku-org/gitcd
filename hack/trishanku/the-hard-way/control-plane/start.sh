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
DATA_BRANCH_BLANK="blank"
METADATA_BRANCH_BLANK="gitcd/metadata/${DATA_BRANCH_BLANK}"

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

function create_volume_if_not_exists {
  local VOLUME="$1"

  docker volume inspect "$VOLUME" > /dev/null 2>&1 || docker volume create --label "$LABEL" "$VOLUME"
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

function start_gitcd {
  local PORT="$1"
  local CONTAINER_PREFIX="$2"
  local REMOTE_REPO="$3"
  local DATA_BRANCH="$4"
  local MERGE_CONFLICT_RESOLUTIONS="$5"
  local NETWORK_ARGS="$6"
  local CONTAINER="${CONTAINER_PREFIX}${DATA_BRANCH}"
  local VOLUME="${CONTAINER}"
  local METADATA_BRANCH="gitcd/metadata/${DATA_BRANCH}"
  local DATA_REF="refs/heads/${DATA_BRANCH}"
  local METADATA_REF="refs/heads/${METADATA_BRANCH}"
  local REMOTE="origin"
  local REMOTE_DATA_BRANCH="${REMOTE}/${DATA_BRANCH}"
  local REMOTE_METADATA_BRANCH="${REMOTE}/${METADATA_BRANCH}"
  local REMOTE_DATA_REF="refs/remotes/${REMOTE_DATA_BRANCH}"
  local REMOTE_METADATA_REF="refs/remotes/${REMOTE_METADATA_BRANCH}"
  local ENTRYPOINT_VOLUME="${CONTAINER}-entrypoint"

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
for file in /secrets/.gitconfig /secrets/.git-credentials; do
  ln -s \$file ~/\$(basename \$file)
done

git clone $REMOTE_REPO /backend

cd /backend

function prepare_branch {
  local BRANCH="\$1"
  local REMOTE_BRANCH="\$2"
  local DEFAULT_BRANCH="\$3"
  local REMOTE="\$4"

  if  git show-branch "\$BRANCH" > /dev/null 2>&1; then
    echo "Branch \${BRANCH} already exists."
  elif git show-branch "\$REMOTE_BRANCH" > /dev/null 2>&1; then
    git branch "\$BRANCH" "\$REMOTE_BRANCH" --track
  else
    git branch "\$BRANCH" "\${DEFAULT_BRANCH}" --no-track \
      && git push -u "\$REMOTE" "\$BRANCH"
  fi
}

prepare_branch "$DATA_BRANCH" "$REMOTE_DATA_BRANCH" "${REMOTE}/${DATA_BRANCH_BLANK}" "$REMOTE"
prepare_branch "$METADATA_BRANCH" "$REMOTE_METADATA_BRANCH" "${REMOTE}/${METADATA_BRANCH_BLANK}" "$REMOTE"
EOF

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
  --data-reference-names=default=${DATA_REF} \
  --metadata-reference-names=default=${METADATA_REF} \
  --key-prefixes=default=/registry \
  --pull-ticker-duration=20s \
  --push-after-merges=default=false \
  --merge-conflict-resolutions=${MERGE_CONFLICT_RESOLUTIONS} \
  --remote-names=default=${REMOTE} \
  --remote-data-reference-names=default=${REMOTE_DATA_REF} \
  --remote-meta-reference-names=default=${REMOTE_METADATA_REF} \
  --listen-urls=default=http://0.0.0.0:${PORT}/ \
  --advertise-client-urls=default=http://127.0.0.1:${PORT}/ \
  --watch-dispatch-channel-size=50
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

function start_apiserver {
  local CONTAINER_PREFIX="$1"
  local DATA_BRANCH="$2"
  local MERGE_CONFLICT_RESOLUTIONS="$3"
  local DOCKER_RUN_ARGS="$4"
  local ETCD_EVENTS_CONTAINER="${CONTAINER_PREFIX}etcd-events-${DATA_BRANCH}"
  local GITCD_CONTAINER_PREFIX="$CONTAINER_PREFIX"
  local NETWORK_ARGS="--network=container:${ETCD_EVENTS_CONTAINER}"
  local APISERVER_CONTAINER="${CONTAINER_PREFIX}kube-apiserver-${DATA_BRANCH}"

  start_etcd "$ETCD_EVENTS_CONTAINER" "$DATA_BRANCH" "$DOCKER_RUN_ARGS"
  start_gitcd "2479" "${GITCD_CONTAINER_PREFIX}the-rest-" "$REPO_THE_REST" "main" "default=2" "$NETWORK_ARGS"
  # start_gitcd "2579" "${GITCD_CONTAINER_PREFIX}nodes-" "$REPO_NODES" "main" "default=2" "$NETWORK_ARGS"
  # start_gitcd "2679" "${GITCD_CONTAINER_PREFIX}leases-" "$REPO_LEASES" "main" "default=2" "$NETWORK_ARGS"
  # start_gitcd "2779" "${GITCD_CONTAINER_PREFIX}priorityclasses-" "$REPO_PRIORITYCLASSES" "main" "default=2" "$NETWORK_ARGS"
  # start_gitcd "2879" "${GITCD_CONTAINER_PREFIX}pods-" "$REPO_PODS" "main" "default=2" "$NETWORK_ARGS"
  # start_gitcd "2979" "${GITCD_CONTAINER_PREFIX}configmaps-" "$REPO_CONFIGMAPS" "main" "default=2" "$NETWORK_ARGS"

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

  fi
}

# store_repo_credentials
start_apiserver "trishanku-the-hard-way-" "main" "default=2" "-p 2379-2380 -p 6443:6443"
# start_apiserver_with_gitcd "trishanku-the-hard-way-test-" "test" "default=2"
