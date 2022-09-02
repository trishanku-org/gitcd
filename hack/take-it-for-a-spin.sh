#!/bin/bash

echo '### Run help and version commands'
echo 
echo '```sh'
echo '# Print the help text.'
echo '$ DOCKER_RUN_OPTS="--rm --name gitcd" make docker-run'
DOCKER_RUN_OPTS="--rm --name gitcd" make docker-run
echo
echo '# Print Gitcd version.'
echo '$ DOCKER_RUN_OPTS="--rm --name gitcd" RUN_ARGS=version make docker-run'
DOCKER_RUN_OPTS="--rm --name gitcd" RUN_ARGS=version make docker-run
echo '```'
echo
echo '### Serve as ETCD with the backend repo in TMPFS'
echo '```sh'
echo '$ RUN_ARGS=serve make docker-run'
RUN_ARGS=serve make docker-run
echo '```'
echo
echo '#### Consume'
echo
echo '```sh'
echo '# Check ETCD endpoint status.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table
echo
echo '# Insert a key and value to create a the main branch that is being used as the backend.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport put /a a'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport put /a a
echo 
echo '# Read all keys.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /
echo
echo '# Check endpoint status.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table
echo
echo '# List ETCD members.'
echo '$ docker run -it --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport member list -w table'
docker run -it --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport member list -w table
echo '```'
echo
echo '#### Cleanup'
echo
echo '```sh'
echo '$ docker stop gitcd'
docker stop gitcd
echo '```'
echo
echo '### Serve ETCD with the backend repo in a separate volume'
echo
echo '```sh'
echo '# Create a volume to store the backend repo.'
echo '$ docker volume create gitcd-backend'
docker volume create gitcd-backend
echo
echo '# Serve as ETCD with the backend repo in the volume.'
echo '$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run'
DOCKER_RUN_OPTS="-d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
echo '```'
echo
echo '#### Consume'
echo
echo '```sh'
echo '# Check if the backend repo has been initialized as an empty and bare Git repo neither any data nor any references.'
echo '$ docker run --rm -v gitcd-backend:/backend busybox ls -R /backend'
docker run --rm -v gitcd-backend:/backend busybox ls -R /backend
echo
echo '# Insert a key and value to create a the main branch that is being used as the backend.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport put /1 1'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport put /1 1
echo
echo '# Read all keys.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix / -w fields'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix / -w fields
echo
echo '# Check that the main branch is now initialized with one commit.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main
echo
echo '# Check the diff for the first commit.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
echo
echo '# Replace one key and put a new key in one transaction.'
cat <<CONTENT_DONE
$ docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport txn -w fields <<EOF

put --prev-kv /1 one
put --prev-kv /2 2


EOF
CONTENT_DONE
docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport txn -w fields <<EOF

put --prev-kv /1 one
put --prev-kv /2 2


EOF
echo
echo '# Read all keys at the latest revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /
echo
echo '# Check that the main branch is now advanced by a commit.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main
echo
echo '# Check the diff for the new commit.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
echo
echo '# Read all keys at the previous revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=1 --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=1 --prefix /
echo
echo '# Delete one key and put a new key in one transaction.'
cat <<CONTENT_DONE
$ docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport txn -w fields <<EOF
value("/1") = "1"


del --prev-kv /2
put --prev-kv /3/3 33

EOF
CONTENT_DONE
docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport txn -w fields <<EOF
value("/1") = "1"


del --prev-kv /2
put --prev-kv /3/3 33

EOF
echo
echo '# Read all keys at the latest revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /
echo
echo '# Check that the main branch is now advanced by a commit.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main
echo
echo '# Check the diff for the new commit.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
echo
echo '# Read all keys at the previous revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=2 --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=2 --prefix /
echo
echo '# Read all keys at the first revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=1 --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=1 --prefix /
echo '```'
echo
echo '#### Cleanup'
echo
echo '```sh'
echo '# Stop the Gitcd container.'
echo '$ docker stop gitcd'
docker stop gitcd
echo
echo '# Remove the volume backing the backend Git repo.'
echo '$ docker volume rm gitcd-backend'
docker volume rm gitcd-backend
echo '```'
echo
echo '### Serve ETCD from an existing Git repo'
echo
echo '```sh'
echo '# Create a volume to store the backend repo.'
echo '$ docker volume create gitcd-backend'
docker volume create gitcd-backend
echo
echo '# Clone a Git repo into the volume to prepare it to be served.'
echo '# NOTE: This might take a while.'
echo '$ docker run --rm -v gitcd-backend:/backend bitnami/git:2 git clone https://github.com/etcd-io/etcd /backend'
docker run --rm -v gitcd-backend:/backend bitnami/git:2 git clone https://github.com/etcd-io/etcd /backend
echo
echo '# Check that the repo got cloned properly.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main -n 2'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main -n 2
echo
echo '# Init the ETCD metadata for the main branch to prepare it to be served.'
echo '# NOTE: This might take a while because the whole commit history of the main branch is traversed to generate the corresponding metadata.'
echo '$ DOCKER_RUN_OPTS="--rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=init make docker-run'
DOCKER_RUN_OPTS="--rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=init make docker-run
echo
echo '# Check that the metadata has been prepared.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log refs/gitcd/metadata/main -n 2'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log refs/gitcd/metadata/main -n 2
echo
echo '# Check that the main branch is unmodified.'
echo '$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main -n 2'
docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main -n 2
echo
echo '# Serve the prepared repo as ETCD.'
echo '$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run'
DOCKER_RUN_OPTS="-d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
echo '```'
echo
echo '#### Consume'
echo
echo '```sh'
echo '# Check ETCD endpoint status.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table
echo
echo '# List main.go keys.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --keys-only --prefix / | grep main.go'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --keys-only --prefix / | grep main.go
echo
echo '# Read /etcdctl/main.go key.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go | tail -n 20'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go | tail -n 20
echo
echo '# Read metadata for /etcdctl/main.go key.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go -w fields | grep -e Key -e Revision -e Version | grep -v Value'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go -w fields | grep -e Key -e Revision -e Version | grep -v Value
echo
echo '# Check the difference between the current and previous versions of the value for /etcdctl/main.go key.'
echo '$ diff <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go ) <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=6425 /etcdctl/main.go )'
diff <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go ) <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=6425 /etcdctl/main.go )
echo '```'
echo
echo '#### Cleanup'
echo
echo '```sh'
echo '# Stop the Gitcd container.'
echo '$ docker stop gitcd'
docker stop gitcd
echo
echo '# Remove the volume backing the backend Git repo.'
echo '$ docker volume rm gitcd-backend'
docker volume rm gitcd-backend
echo '```'
echo
echo '### Run a local Kubernetes cluster with Gitcd as the backend'
echo
echo '#### Note'
echo
echo 'The following steps will start a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io).'
echo 'Please make sure that there are no other local Kubernetes clusters running (e.g. docker-desktop).'
echo 'These steps also start containers that listen on ports like `2379`, `2479`, `2579`, `2679`, `2779`, `2879`, `2979` and `6443`.'
echo 'Please ensure that these ports are available before running these steps.'
echo
echo '```sh'
echo '# Start a local Kind Kubernetes cluster with Gitcd as the backend.'
echo '$ make start-docker-gitcd-kind'
make start-docker-gitcd-kind
echo
echo '# Check that Kind and Gitcd containers are running.'
echo '$ docker ps -n 8'
docker ps -n 8
echo
echo '# Check that the kubeconfig is pointint to the newly setup cluster.'
echo '$ kubectl config current-context'
kubectl config current-context
echo
echo '# Check the Kubernetes cluster information.'
echo '$ kubectl cluster-info'
kubectl cluster-info
echo
echo '# List the namespaces in the cluster.'
echo '$ kubectl get namespaces'
kubectl get namespaces
echo
echo '# List the nodes in the cluster.'
echo '$ kubectl get nodes'
kubectl get nodes
echo
echo '# Wait for the node to be Ready.'
echo '$ caffeinate -disu sleep 6m'
caffeinate -disu sleep 6m
echo
echo '# Check if the node is Ready.'
echo '$ kubectl get nodes'
kubectl get nodes
echo
echo '# List all the pods in the cluster across all namespaces.'
echo '$ kubectl get pods --all-namespaces'
kubectl get pods --all-namespaces
echo
echo '# Run a pod to say hello.'
echo '$ kubectl run -i -t hello --image=busybox:1 --restart=Never --rm --pod-running-timeout=3m echo ' "'Hello, World!'"
kubectl run -i -t hello --image=busybox:1 --restart=Never --rm --pod-running-timeout=3m echo 'Hello, World!'
echo
echo '# Inspect the Kubernetes content in the backend Git repo.'
echo '$ echo "git reset --hard && git checkout refs/gitcd/metadata/nodes && git checkout nodes" | \
    docker run -i --rm -v gitcd-nodes:/backend -w /backend bitnami/git:2 sh'
echo "git reset --hard && git checkout refs/gitcd/metadata/nodes && git checkout nodes" | \
    docker run -i --rm -v gitcd-nodes:/backend -w /backend bitnami/git:2 sh
echo
echo '$ docker run --rm -v gitcd-nodes:/backend busybox:1 cat /backend/registry/minions/trishanku-control-plane'
docker run --rm -v gitcd-nodes:/backend busybox:1 cat /backend/registry/minions/trishanku-control-plane
echo
echo '# Check the difference between the content in the backend git repo and the output from kubectl (there might have been more updates in the meantime).'
echo '$ diff -u <( docker run --rm -v gitcd-nodes:/backend busybox:1 cat /backend/registry/minions/trishanku-control-plane ) <( kubectl get node trishanku-control-plane --show-managed-fields=true -oyaml )'
diff -u <( docker run --rm -v gitcd-nodes:/backend busybox:1 cat /backend/registry/minions/trishanku-control-plane ) <( kubectl get node trishanku-control-plane --show-managed-fields=true -oyaml )
echo 
echo '```'
echo
echo '#### Cleanup'
echo
echo '```sh'
echo '# Stop kube-apiserver and Gitcd containers.'
echo '$ make stop-docker-gitcd-kind'
make stop-docker-gitcd-kind
echo
echo '# Clean up kube-apiserver and Gitcd container and volumes.'
echo '$ make cleanup-docker-gitcd-kind'
make cleanup-docker-gitcd-kind
echo '```'
