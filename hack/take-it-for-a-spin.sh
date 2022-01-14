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
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table
echo
echo '# Insert a key and value to create a the main branch that is being used as the backend.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport put /a a'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport put /a a
echo 
echo '# Read all keys.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix /
echo
echo '# Check endpoint status.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table
echo
echo '# List ETCD members.'
echo '$ docker run -it --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport member list -w table'
docker run -it --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport member list -w table
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
echo '$ docker volume create gitcd-backend-repo'
docker volume create gitcd-backend-repo
echo
echo '# Serve as ETCD with the backend repo in the volume.'
echo '$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run'
DOCKER_RUN_OPTS="-d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
echo '```'
echo
echo '#### Consume'
echo
echo '```sh'
echo '# Check if the backend repo has been initialized as an empty and bare Git repo neither any data nor any references.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend busybox ls -R /backend'
docker run --rm -v gitcd-backend-repo:/backend busybox ls -R /backend
echo
echo '# Insert a key and value to create a the main branch that is being used as the backend.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport put /1 1'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport put /1 1
echo
echo '# Read all keys.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix / -w fields'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix / -w fields
echo
echo '# Check that the main branch is now initialized with one commit.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main
echo
echo '# Check the diff for the first commit.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
echo
echo '# Replace one key and put a new key in one transaction.'
cat <<CONTENT_DONE
$ docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport txn -w fields <<EOF

put --prev-kv /1 one
put --prev-kv /2 2


EOF
CONTENT_DONE
docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport txn -w fields <<EOF

put --prev-kv /1 one
put --prev-kv /2 2


EOF
echo
echo '# Read all keys at the latest revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix /
echo
echo '# Check that the main branch is now advanced by a commit.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main
echo
echo '# Check the diff for the new commit.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
echo
echo '# Read all keys at the previous revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=1 --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=1 --prefix /
echo
echo '# Delete one key and put a new key in one transaction.'
cat <<CONTENT_DONE
$ docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport txn -w fields <<EOF
value("/1") = "1"


del --prev-kv /2
put --prev-kv /3/3 33

EOF
CONTENT_DONE
docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport txn -w fields <<EOF
value("/1") = "1"


del --prev-kv /2
put --prev-kv /3/3 33

EOF
echo
echo '# Read all keys at the latest revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix /
echo
echo '# Check that the main branch is now advanced by a commit.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main
echo
echo '# Check the diff for the new commit.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
echo
echo '# Read all keys at the previous revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=2 --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=2 --prefix /
echo
echo '# Read all keys at the first revision.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=1 --prefix /'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=1 --prefix /
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
echo '$ docker volume rm gitcd-backend-repo'
docker volume rm gitcd-backend-repo
echo '```'
echo
echo '# Serve ETCD from an existing Git repo'
echo
echo '```sh'
echo '# Create a volume to store the backend repo.'
echo '$ docker volume create gitcd-backend-repo'
docker volume create gitcd-backend-repo
echo
echo '# Clone a Git repo into the volume to prepare it to be served.'
echo '# NOTE: This might take a while.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend bitnami/git git clone https://github.com/etcd-io/etcd /backend'
docker run --rm -v gitcd-backend-repo:/backend bitnami/git git clone https://github.com/etcd-io/etcd /backend
echo
echo '# Check that the repo got cloned properly.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main -n 2'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main -n 2
echo
echo '# Init the ETCD metadata for the main branch to prepare it to be served.'
echo '# NOTE: This might take a while because the whole commit history of the main branch is traversed to generate the corresponding metadata.'
echo '$ DOCKER_RUN_OPTS="--rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=init make docker-run'
DOCKER_RUN_OPTS="--rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=init make docker-run
echo
echo '# Check that the metadata has been prepared.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log refs/gitcd/metadata/refs/heads/main -n 2'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log refs/gitcd/metadata/refs/heads/main -n 2
echo
echo '# Check that the main branch is unmodified.'
echo '$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main -n 2'
docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main -n 2
echo
echo '# Serve the prepared repo as ETCD.'
echo '$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run'
DOCKER_RUN_OPTS="-d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
echo '```'
echo
echo '#### Consume'
echo
echo '```sh'
echo '# Check ETCD endpoint status.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table
echo
echo '# List main.go keys.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --keys-only --prefix / | grep main.go'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --keys-only --prefix / | grep main.go
echo
echo '# Read /etcdctl/main.go key.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go | tail -n 20'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go | tail -n 20
echo
echo '# Read metadata for /etcdctl/main.go key.'
echo '$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go -w fields | grep -e Key -e Revision -e Version | grep -v Value'
docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go -w fields | grep -e Key -e Revision -e Version | grep -v Value
echo
echo '# Check the difference between the current and previous versions of the value for /etcdctl/main.go key.'
echo '$ diff <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go ) <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=6425 /etcdctl/main.go )'
diff <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go ) <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=6425 /etcdctl/main.go )
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
echo '$ docker volume rm gitcd-backend-repo'
docker volume rm gitcd-backend-repo
echo '```'