#!/bin/bash

SECRETS_VOLUME_NAME="kubernetes-the-hard-way-secrets"
REMOTE_REPO="https://github.com/amshuman-kr/trishanku-the-hard-way"
USER=
PASSWORD=

LABEL="trishanku=the-hard-way"
ETCD_NAME="kubernetes-the-hard-way"
ETCD_CONTAINER_NAME=etcd
GIT_IMG=bitnami/git:2
GITCD_IMG=trishanku/gitcd:latest
DATA_BRANCH_BLANK="main"
METADATA_BRANCH_BLANK="gitcd/metadata/${DATA_BRANCH_BLANK}"

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

BUSYBOX_IMG=busybox:1

function store_repo_credentials {
  read -p "User for the repo ${REMOTE_REPO}: " USER
  read -sp "Password for the repo ${REMOTE_REPO}: " PASSWORD
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

function start_apiserver_with_gitcd {
  local CONTAINER_PREFIX="$1"
  local DATA_BRANCH_NAME="$2"
  local MERGE_CONFLICT_RESOLUTIONS="$3"
  local DOCKER_RUN_ARGS="$4"
  local GITCD_CONTAINER_NAME="${CONTAINER_PREFIX}gitcd"
  local GITCD_VOLUME_NAME="$GITCD_CONTAINER_NAME"
  local METADATA_BRANCH_NAME="gitcd/metadata/${DATA_BRANCH_NAME}"
  local DATA_REF_NAME="refs/heads/${DATA_BRANCH_NAME}"
  local METADATA_REF_NAME="refs/heads/${METADATA_BRANCH_NAME}"
  local REMOTE_NAME="origin"
  local REMOTE_DATA_BRANCH_NAME="${REMOTE_NAME}/${DATA_BRANCH_NAME}"
  local REMOTE_METADATA_BRANCH_NAME="${REMOTE_NAME}/${METADATA_BRANCH_NAME}"
  local REMOTE_DATA_REF_NAME="refs/remotes/${REMOTE_DATA_BRANCH_NAME}"
  local REMOTE_METADATA_REF_NAME="refs/remotes/${REMOTE_METADATA_BRANCH_NAME}"
  local ENTRYPOINT_VOLUME_NAME="${GITCD_CONTAINER_NAME}-entrypoint"
  local APISERVER_CONTAINER_NAME="${CONTAINER_PREFIX}kube-apiserver"

  if docker inspect $GITCD_CONTAINER_NAME > /dev/null 2>&1; then
    echo "Container ${GITCD_CONTAINER_NAME} already exists."
  else
    docker volume create --label "$LABEL" "$GITCD_VOLUME_NAME"

    cat <<EOF | \
      docker run --name "$GITCD_CONTAINER_NAME" \
        -i --rm \
        --label "$LABEL" \
        -v "${GITCD_VOLUME_NAME}:/backend" \
        -v "${SECRETS_VOLUME_NAME}:/secrets" \
        $GIT_IMG bash
for file in /secrets/.gitconfig /secrets/.git-credentials; do
  ln -s \$file ~/\$(basename \$file)
done
git clone $REMOTE_REPO /backend
EOF
  
    cat << EOF | \
      docker run --name "$GITCD_CONTAINER_NAME" \
        -i --rm \
        --label "$LABEL" \
        -v "${GITCD_VOLUME_NAME}:/backend" \
        -v "${SECRETS_VOLUME_NAME}:/secrets" \
        -w /backend \
        $GIT_IMG bash
for file in /secrets/.gitconfig /secrets/.git-credentials; do
  ln -s \$file ~/\$(basename \$file)
done

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

prepare_branch "$DATA_BRANCH_NAME" "$REMOTE_DATA_BRANCH_NAME" "${REMOTE_NAME}/${DATA_BRANCH_BLANK}" "$REMOTE_NAME"
prepare_branch "$METADATA_BRANCH_NAME" "$REMOTE_METADATA_BRANCH_NAME" "${REMOTE_NAME}/${METADATA_BRANCH_BLANK}" "$REMOTE_NAME"
EOF

    docker volume create --label "$LABEL" "$ENTRYPOINT_VOLUME_NAME"

    cat <<EOF | \
      docker run --name "$GITCD_CONTAINER_NAME" \
        -i --rm \
        --label "$LABEL" \
        -v "${ENTRYPOINT_VOLUME_NAME}:/entrypoint" \
        $BUSYBOX_IMG sh
cat <<INNER_EOF > /entrypoint/entrypoint.sh
#!/bin/sh

for file in /secrets/.gitconfig /secrets/.git-credentials; do
  target="\\\${HOME}/\\\$(basename \\\$file)"
  test -L \\\$target || ln -s \\\$file \\\$target
done

exec /gitcd serve \
  --repo=/backend \
  --data-reference-names=default=${DATA_REF_NAME} \
  --metadata-reference-names=default=${METADATA_REF_NAME} \
  --key-prefixes=default=/registry \
  --pull-ticker-duration=20s \
  --push-after-merges=default=true \
  --merge-conflict-resolutions=${MERGE_CONFLICT_RESOLUTIONS} \
  --remote-names=default=${REMOTE_NAME} \
  --remote-data-reference-names=default=${REMOTE_DATA_REF_NAME} \
  --remote-meta-reference-names=default=${REMOTE_METADATA_REF_NAME}
INNER_EOF

chmod +x /entrypoint/entrypoint.sh
EOF

    docker run --name "$GITCD_CONTAINER_NAME" \
      -d --restart=unless-stopped \
      --label "$LABEL" \
      $DOCKER_RUN_ARGS \
      -v "${GITCD_VOLUME_NAME}:/backend" \
      -v "${SECRETS_VOLUME_NAME}:/secrets" \
      -v "${ENTRYPOINT_VOLUME_NAME}:/entrypoint" \
      --entrypoint= \
      $GITCD_IMG /entrypoint/entrypoint.sh

    if [ "$LOCAL_IP" == "" ]; then
      LOCAL_IP="127.0.0.1"
    fi

    if [ "$INTERNAL_IP" == "" ]; then
      INTERNAL_IP="$(ifconfig | awk '/inet / {split($2,var,"/*"); print var[1]}' | grep -v 127.0.0.1 | head -n 1)"
    fi

    if  [ "$KUBERNETES_PUBLIC_ADDRESS" == "" ]; then
      KUBERNETES_PUBLIC_ADDRESS="$INTERNAL_IP"
    fi

    docker run --name $APISERVER_CONTAINER_NAME \
      -d --restart=unless-stopped \
      --label "$LABEL" \
      "--network=container:$GITCD_CONTAINER_NAME" \
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
        --etcd-compaction-interval=0 \
        --etcd-keyfile=/secrets/kubernetes-key.pem \
        --etcd-servers=http://${LOCAL_IP}:2379 \
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
        --storage-media-type=application/yaml \
        --tls-cert-file=/secrets/kubernetes.pem \
        --tls-private-key-file=/secrets/kubernetes-key.pem \
        --v=2 \
        --watch-cache=false

  fi
}

# store_repo_credentials
start_apiserver_with_gitcd "trishanku-the-hard-way-main-" "main" "default=2" "-p 6443:6443"
# start_apiserver_with_gitcd "trishanku-the-hard-way-test-" "test" "default=2"
