# Gitcd

Gitcd - Git as a distributed key-value store.

## Content

- [Gitcd](#gitcd)
- [Content](#content)
- [Take it for a spin](#take-it-for-a-spin)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Run help and version commands](#run-help-and-version-commands)
  - [Serve as ETCD with the backend repo in TMPFS](#serve-as-etcd-with-the-backend-repo-in-tmpfs)
    - [Consume](#consume)
    - [Cleanup](#cleanup)
  - [Serve ETCD with the backend repo in a separate volume](#serve-etcd-with-the-backend-repo-in-a-separate-volume)
    - [Consume](#consume-1)
    - [Cleanup](#cleanup-1)
  - [Serve ETCD from an existing Git repo](#serve-etcd-from-an-existing-git-repo)
    - [Consume](#consume-2)
    - [Cleanup](#cleanup-2)

## Take it for a spin

### Prerequisites

1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
1. [Docker](https://docs.docker.com/engine/install/)
1. [Kind](https://kind.sigs.k8s.io), a tool for running local Kubernetes clusters using Docker container "nodes"
1. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/#install-with-homebrew-on-macos), the Kubernetes command-line tool
1. [Caffeinate](https://ss64.com/osx/caffeinate.html) to keep the laptop from going to sleep during some long running steps below

### Build

```sh
# Clone the Git repo somewhere locally.
$ git clone https://github.com/trishanku-org/gitcd <somewhere>

# Move to the checked out repo.
$ cd <somewhere>

# Build the docker image.
$ make docker-build
docker build -t "asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest" .
[+] Building 63.3s (18/18) FINISHED
```

### Run help and version commands

```sh
# Print the help text.
$ DOCKER_RUN_OPTS="--rm --name gitcd" make docker-run
docker run --rm --name gitcd "asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest"
Gitcd - Git as a distributed key-value store.

Usage:
  gitcd [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  init        Init a Git repo for use as a backend.
  pull        Pull changes from remote into the backend.
  serve       Start GRPC service compatible with any ETCD client.
  version     Prints the version of Gitcd.

Flags:
      --debug            Enable debug logging
  -h, --help             help for gitcd
      --verbosity int8   Logging verbosity

Use "gitcd [command] --help" for more information about a command.

# Print Gitcd version.
$ DOCKER_RUN_OPTS="--rm --name gitcd" RUN_ARGS=version make docker-run
docker run --rm --name gitcd "asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest" version
Gitcd: v0.0.1-dev
```

### Serve as ETCD with the backend repo in TMPFS

```sh
$ RUN_ARGS=serve make docker-run
docker run -d --rm --tmpfs /tmp/trishanku/gitcd:rw,noexec,nosuid,size=65536k --name gitcd "asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest" serve
d203235df099c426324065636a1c0f62250d8816edc2b65792eb9f8bfdde28bb
```

#### Consume

```sh
# Check ETCD endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table
{"level":"warn","ts":"2023-08-13T17:07:00.897Z","logger":"etcd-client","caller":"v3/retry_interceptor.go:62","msg":"retrying of unary invoker failed","target":"etcd-endpoints://0xc0001ca000/127.0.0.1:2379","attempt":0,"error":"rpc error: code = Unknown desc = reference 'refs/gitcd/metadata/main' not found"}
Failed to get the status of endpoint 127.0.0.1:2379 (rpc error: code = Unknown desc = reference 'refs/gitcd/metadata/main' not found)
+----------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
| ENDPOINT | ID | VERSION | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+----------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
+----------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+

# Insert a key and value to create a the main branch that is being used as the backend.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport put /a a
OK

# Read all keys.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /
/a
a

# Check endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table
+----------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
|    ENDPOINT    | ID | VERSION | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+----------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
| 127.0.0.1:2379 |  0 |         |  2.7 kB |      true |      false |         1 |          1 |                  1 |        |
+----------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+

# List ETCD members.
$ docker run -it --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport member list -w table
+----+---------+-----------+------------+------------------------+------------+
| ID | STATUS  |   NAME    | PEER ADDRS |      CLIENT ADDRS      | IS LEARNER |
+----+---------+-----------+------------+------------------------+------------+
|  0 | started | trishanku |            | http://127.0.0.1:2379/ |      false |
+----+---------+-----------+------------+------------------------+------------+
```

#### Cleanup

```sh
$ docker stop gitcd
gitcd
```

### Serve ETCD with the backend repo in a separate volume

```sh
# Create a volume to store the backend repo.
$ docker volume create gitcd-backend
gitcd-backend

# Serve as ETCD with the backend repo in the volume.
$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
docker run -d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd "asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest" serve
a78b32a816f39fdbc1f68c826208fd14dfedf6ef3b9016e22164f3f1249a75ab
```

#### Consume

```sh
# Check if the backend repo has been initialized as an empty and bare Git repo neither any data nor any references.
$ docker run --rm -v gitcd-backend:/backend busybox ls -R /backend
/backend:
HEAD
config
description
hooks
info
objects
refs

/backend/hooks:
README.sample

/backend/info:
exclude

/backend/objects:
info
pack

/backend/objects/info:

/backend/objects/pack:

/backend/refs:
heads
tags

/backend/refs/heads:

/backend/refs/tags:

# Insert a key and value to create a the main branch that is being used as the backend.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport put /1 1
OK

# Read all keys.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix / -w fields
"ClusterID" : 0
"MemberID" : 0
"Revision" : 1
"RaftTerm" : 1
"Key" : "/1"
"CreateRevision" : 1
"ModRevision" : 1
"Version" : 1
"Value" : "1"
"Lease" : 0
"More" : false
"Count" : 1

# Check that the main branch is now initialized with one commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main
commit 778e86a08f99d94d8ae9cb7418f6ff4cc40ffbfb
Author: trishanku <trishanku@heaven.com>
Date:   Sun Aug 13 17:10:06 2023 +0000

    1

# Check the diff for the first commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
commit 778e86a08f99d94d8ae9cb7418f6ff4cc40ffbfb
tree 3f258de18b33904bb291ad94cc7d60e4c197fd50
author trishanku <trishanku@heaven.com> 1691946606 +0000
committer trishanku <trishanku@heaven.com> 1691946606 +0000

    1

diff --git a/1 b/1
new file mode 100644
index 0000000..56a6051
--- /dev/null
+++ b/1
@@ -0,0 +1 @@
+1
\ No newline at end of file

# Replace one key and put a new key in one transaction.
$ docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport txn -w fields <<EOF

put --prev-kv /1 one
put --prev-kv /2 2


EOF
"ClusterID" : 0
"MemberID" : 0
"Revision" : 2
"RaftTerm" : 2
"Succeeded" : true
"ClusterID" : 0
"MemberID" : 0
"Revision" : 2
"RaftTerm" : 2
"PrevKey" : "/1"
"PrevCreateRevision" : 1
"PrevModRevision" : 1
"PrevVersion" : 1
"PrevValue" : "1"
"PrevLease" : 0
"ClusterID" : 0
"MemberID" : 0
"Revision" : 2
"RaftTerm" : 2

# Read all keys at the latest revision.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /
/1
one
/2
2

# Check that the main branch is now advanced by a commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main
commit d83e325a85344820d69b35683e7d7f5f55481477
Author: trishanku <trishanku@heaven.com>
Date:   Sun Aug 13 17:11:07 2023 +0000

    2

commit 778e86a08f99d94d8ae9cb7418f6ff4cc40ffbfb
Author: trishanku <trishanku@heaven.com>
Date:   Sun Aug 13 17:10:06 2023 +0000

    1

# Check the diff for the new commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
commit d83e325a85344820d69b35683e7d7f5f55481477
tree 904b9644b5f7f0bc10a4d52850994bb58ac1591e
parent 778e86a08f99d94d8ae9cb7418f6ff4cc40ffbfb
author trishanku <trishanku@heaven.com> 1691946667 +0000
committer trishanku <trishanku@heaven.com> 1691946667 +0000

    2

diff --git a/1 b/1
index 56a6051..43dd47e 100644
--- a/1
+++ b/1
@@ -1 +1 @@
-1
\ No newline at end of file
+one
\ No newline at end of file
diff --git a/2 b/2
new file mode 100644
index 0000000..d8263ee
--- /dev/null
+++ b/2
@@ -0,0 +1 @@
+2
\ No newline at end of file

# Read all keys at the previous revision.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=1 --prefix /
/1
1

# Delete one key and put a new key in one transaction.
$ docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport txn -w fields <<EOF
value("/1") = "1"


del --prev-kv /2
put --prev-kv /3/3 33

EOF
"ClusterID" : 0
"MemberID" : 0
"Revision" : 3
"RaftTerm" : 3
"Succeeded" : false
"ClusterID" : 0
"MemberID" : 0
"Revision" : 3
"RaftTerm" : 3
"Deleted" : 1
"PrevKey" : "/2"
"PrevCreateRevision" : 2
"PrevModRevision" : 2
"PrevVersion" : 1
"PrevValue" : "2"
"PrevLease" : 0
"ClusterID" : 0
"MemberID" : 0
"Revision" : 3
"RaftTerm" : 3

# Read all keys at the latest revision.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --prefix /
/1
one
/3/3
33

# Check that the main branch is now advanced by a commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main
commit dab53285239d02c773f0c41e9f21856c8e029ecb
Author: trishanku <trishanku@heaven.com>
Date:   Sun Aug 13 17:12:11 2023 +0000

    3

commit d83e325a85344820d69b35683e7d7f5f55481477
Author: trishanku <trishanku@heaven.com>
Date:   Sun Aug 13 17:11:07 2023 +0000

    2

commit 778e86a08f99d94d8ae9cb7418f6ff4cc40ffbfb
Author: trishanku <trishanku@heaven.com>
Date:   Sun Aug 13 17:10:06 2023 +0000

    1

# Check the diff for the new commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
commit dab53285239d02c773f0c41e9f21856c8e029ecb
tree be974b5190bf4ca3f70d8474e2b5bbcbfda5156b
parent d83e325a85344820d69b35683e7d7f5f55481477
author trishanku <trishanku@heaven.com> 1691946731 +0000
committer trishanku <trishanku@heaven.com> 1691946731 +0000

    3

diff --git a/2 b/2
deleted file mode 100644
index d8263ee..0000000
--- a/2
+++ /dev/null
@@ -1 +0,0 @@
-2
\ No newline at end of file
diff --git a/3/3 b/3/3
new file mode 100644
index 0000000..dc7b54a
--- /dev/null
+++ b/3/3
@@ -0,0 +1 @@
+33
\ No newline at end of file

# Read all keys at the previous revision.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=2 --prefix /
/1
one
/2
2

# Read all keys at the first revision.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=1 --prefix /
/1
1
```

#### Cleanup

```sh
# Stop the Gitcd container.
$ docker stop gitcd
gitcd

# Remove the volume backing the backend Git repo.
$ docker volume rm gitcd-backend
gitcd-backend
```

### Serve ETCD from an existing Git repo

```sh
# Create a volume to store the backend repo.
$ docker volume create gitcd-backend
gitcd-backend

# Clone a Git repo into the volume to prepare it to be served.
# NOTE: This might take a while.
$ docker run --rm -v gitcd-backend:/backend bitnami/git:2 git clone https://github.com/etcd-io/etcd /backend
Cloning into '/backend'...

# Check that the repo got cloned properly.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main -n 2
commit 1edd8baf3c6024eceb8b0d45062f0cebf7be9bc5
Merge: baf7a34c3 b6d123d08
Author: Benjamin Wang <wachao@vmware.com>
Date:   Fri Aug 11 07:59:34 2023 +0100

    Merge pull request #16394 from jmhbnz/update-to-go-1.20

    Migrate to golang 1.20

commit b6d123d08b56dfc16c657dcd59f6d1bca800a6f0
Author: James Blair <mail@jamesblair.net>
Date:   Fri Aug 11 15:03:48 2023 +1200

    Update to golang 1.20 minor release.

    Signed-off-by: James Blair <mail@jamesblair.net>

# Init the ETCD metadata for the main branch to prepare it to be served.
# NOTE: This might take a while because the whole commit history of the main branch is traversed to generate the corresponding metadata.
$ DOCKER_RUN_OPTS="--rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=init make docker-run
docker run --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd "asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest" init
{"level":"info","ts":1691946916.5496962,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/tmp/trishanku/gitcd","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefName":"refs/gitcd/metadata/main","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1691947070.8866885,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}

# Check that the metadata has been prepared.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log refs/gitcd/metadata/main -n 2
commit 066644237e92774c186ff5f51ac77925f631997b
Merge: 0ee2c6df1 944317b30
Author: trishanku <trishanku@heaven.com>
Date:   Sun Aug 13 17:17:50 2023 +0000

    7919

commit 0ee2c6df17a57e4b2902bfa5f1b8313079c49e3d
Merge: 9067d7797 f6a3965e6
Author: trishanku <trishanku@heaven.com>
Date:   Sun Aug 13 17:17:50 2023 +0000

    7918

# Check that the main branch is unmodified.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main -n 2
commit 1edd8baf3c6024eceb8b0d45062f0cebf7be9bc5
Merge: baf7a34c3 b6d123d08
Author: Benjamin Wang <wachao@vmware.com>
Date:   Fri Aug 11 07:59:34 2023 +0100

    Merge pull request #16394 from jmhbnz/update-to-go-1.20

    Migrate to golang 1.20

commit b6d123d08b56dfc16c657dcd59f6d1bca800a6f0
Author: James Blair <mail@jamesblair.net>
Date:   Fri Aug 11 15:03:48 2023 +1200

    Update to golang 1.20 minor release.

    Signed-off-by: James Blair <mail@jamesblair.net>

# Serve the prepared repo as ETCD.
$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
docker run -d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd "asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest" serve
d8069c3680c4e1480ef30bf1ae8f016e035f9cbb7ff105609198fbb752f8d45c
```

#### Consume

```sh
# Check ETCD endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+
|    ENDPOINT    | ID |  VERSION   | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+
| 127.0.0.1:2379 |  0 | v0.0.1-dev |  170 MB |      true |      false |      7919 |       7919 |               7919 |        |
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+

# List main.go keys.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --keys-only --prefix / | grep main.go
/contrib/raftexample/main.go
/etcdctl/main.go
/etcdutl/main.go
/server/etcdmain/main.go
/server/main.go
/tools/benchmark/main.go
/tools/etcd-dump-db/main.go
/tools/etcd-dump-logs/main.go
/tools/etcd-dump-metrics/main.go
/tools/proto-annotations/main.go

# Read /etcdctl/main.go key.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go | tail -n 20

import (
	"go.etcd.io/etcd/etcdctl/v3/ctlv3"
)

/*
*
mainWithError is fully analogous to main, but instead of signaling errors
by os.Exit, it exposes the error explicitly, such that test-logic can intercept
control to e.g. dump coverage data (even for test-for-failure scenarios).
*/
func mainWithError() error {
	return ctlv3.Start()
}

func main() {
	ctlv3.MustStart()
	return
}


# Read metadata for /etcdctl/main.go key.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go -w fields | grep -e Key -e Revision -e Version | grep -v Value
"Revision" : 7919
"Key" : "/etcdctl/main.go"
"CreateRevision" : 818
"ModRevision" : 7226
"Version" : 38

# Check the difference between the current and previous versions of the value for /etcdctl/main.go key.
$ diff <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go ) <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=7225 /etcdctl/main.go )
23,24c23
< /*
< *
---
> /**
```

#### Cleanup

```sh
# Stop the Gitcd container.
$ docker stop gitcd
gitcd

# Remove the volume backing the backend Git repo.
$ docker volume rm gitcd-backend
gitcd-backend
```

### Run kube-apiserver with Gitcd as the backend
```sh
# Start kube-apiserver and Gitcd containers.
$ make start-docker-gitcd-kube-apiserver
hack/kube/start.sh
+ start_gitcd
+ docker volume create gitcd-backend
gitcd-backend
+ echo 'git init -b main && git config user.email "trishanku@heaven.com" && git config user.name trishanku'
+ docker run -i --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 sh
Initialized empty Git repository in /backend/.git/
+ echo 'touch init && git add init && git commit -m init'
+ docker run -i --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 sh
[main (root-commit) d2c5b2e] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init
+ docker run --rm -v gitcd-backend:/backend asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest init --repo=/backend
{"level":"info","ts":1692002025.9472187,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefName":"refs/gitcd/metadata/main","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1692002025.9550986,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
+ docker run --name gitcd -d -v gitcd-backend:/backend asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest serve --repo=/backend --debug
a92ec60a5de48f628d0a89bc09272f7d4a636b62fd3254e4b9f6570ef590e8d1
+ docker run --rm --network=container:gitcd --entrypoint etcdctl bitnami/etcd:3 --insecure-transport endpoint status
127.0.0.1:2379, 0, v0.0.1-dev, 152 kB, true, false, 1, 1, 1,
+ prepare_certs
+ docker volume create kube-certs
kube-certs
+ docker run -i --rm -v kube-certs:/tls -w /tls busybox:1 dd of=tls.conf
0+1 records in
0+1 records out
482 bytes (482B) copied, 0.003055 seconds, 154.1KB/s
+ docker run --rm -v kube-certs:/tls -w /tls --entrypoint openssl nginx:1 req -newkey rsa:4096 -x509 -sha256 -days 365 -nodes -extensions v3_req -config tls.conf -out tls.crt -keyout tls.key
Generating a RSA private key
.....................................++++
........................................++++
writing new private key to 'tls.key'
-----
+ docker run --rm -v kube-certs:/tls -w /tls busybox:1 ln -s tls.crt ca.crt
+ start_kube_apiserver
+ docker run --name kube-apiserver -d -v kube-certs:/tls --network=container:gitcd --entrypoint kube-apiserver k8s.gcr.io/kube-apiserver:v1.24.4 --client-ca-file=/tls/ca.crt --etcd-compaction-interval=0 --etcd-servers=http://localhost:2379 --secure-port=6443 --service-account-issuer=https://kube-apiserver/ --service-account-key-file=/tls/tls.key --service-account-signing-key-file=/tls/tls.key --storage-media-type=application/yaml --tls-cert-file=/tls/tls.crt --tls-private-key-file=/tls/tls.key --watch-cache=false
Unable to find image 'k8s.gcr.io/kube-apiserver:v1.24.4' locally
v1.24.4: Pulling from kube-apiserver
Digest: sha256:74496d788bad4b343b2a2ead2b4ac8f4d0d99c45c451b51c076f22e52b84f1e5
Status: Downloaded newer image for k8s.gcr.io/kube-apiserver:v1.24.4
e6d158dfb184a5b803753c860d307d6c8e29b6639a5a1218f92486363931663e

# Check that kube-apiserver and Gitcd containers are running.
$ docker ps -n 2
CONTAINER ID   IMAGE                                                         COMMAND                  CREATED         STATUS                  PORTS     NAMES
e6d158dfb184   k8s.gcr.io/kube-apiserver:v1.24.4                             "kube-apiserver --cl…"   1 second ago    Up Less than a second             kube-apiserver
a92ec60a5de4   asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest   "/gitcd serve --repo…"   5 seconds ago   Up 4 seconds                      gitcd

# Wait for kube-apiserver to initialize.
$ sleep 10

# Inspect the Kubernetes content in the backend Git repo.
$ echo "git reset --hard && git checkout refs/gitcd/metadata/refs/heads/main && git checkout main" | \
    docker run -i --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 sh
HEAD is now at 9aadc87 80
error: pathspec 'refs/gitcd/metadata/refs/heads/main' did not match any file(s) known to git

$ echo "apk update && apk add tree && tree /backend" | docker run -i --rm -v gitcd-backend:/backend alpine:3 sh
fetch https://dl-cdn.alpinelinux.org/alpine/v3.15/main/x86_64/APKINDEX.tar.gz
fetch https://dl-cdn.alpinelinux.org/alpine/v3.15/community/x86_64/APKINDEX.tar.gz
v3.15.10-11-g237beaf9c41 [https://dl-cdn.alpinelinux.org/alpine/v3.15/main]
v3.15.10-11-g237beaf9c41 [https://dl-cdn.alpinelinux.org/alpine/v3.15/community]
OK: 15866 distinct packages available
(1/1) Installing tree (1.8.0-r0)
Executing busybox-1.34.1-r3.trigger
OK: 6 MiB in 15 packages
/backend
├── init
└── registry
    ├── apiregistration.k8s.io
    │   └── apiservices
    │       ├── v1.
    │       ├── v1.admissionregistration.k8s.io
    │       ├── v1.apiextensions.k8s.io
    │       ├── v1.apps
    │       ├── v1.authentication.k8s.io
    │       ├── v1.authorization.k8s.io
    │       ├── v1.autoscaling
    │       ├── v1.batch
    │       ├── v1.certificates.k8s.io
    │       ├── v1.coordination.k8s.io
    │       ├── v1.discovery.k8s.io
    │       ├── v1.events.k8s.io
    │       ├── v1.networking.k8s.io
    │       ├── v1.node.k8s.io
    │       ├── v1.policy
    │       ├── v1.rbac.authorization.k8s.io
    │       ├── v1.scheduling.k8s.io
    │       ├── v1.storage.k8s.io
    │       ├── v1beta1.batch
    │       ├── v1beta1.discovery.k8s.io
    │       ├── v1beta1.events.k8s.io
    │       ├── v1beta1.flowcontrol.apiserver.k8s.io
    │       ├── v1beta1.node.k8s.io
    │       ├── v1beta1.policy
    │       ├── v1beta1.storage.k8s.io
    │       ├── v1beta2.flowcontrol.apiserver.k8s.io
    │       ├── v2.autoscaling
    │       ├── v2beta1.autoscaling
    │       └── v2beta2.autoscaling
    ├── configmaps
    │   └── kube-system
    │       └── extension-apiserver-authentication
    ├── endpointslices
    │   └── default
    │       └── kubernetes
    ├── flowschemas
    │   ├── catch-all
    │   ├── endpoint-controller
    │   ├── exempt
    │   ├── global-default
    │   ├── kube-controller-manager
    │   ├── kube-scheduler
    │   ├── kube-system-service-accounts
    │   ├── probes
    │   ├── service-accounts
    │   ├── system-leader-election
    │   ├── system-node-high
    │   ├── system-nodes
    │   └── workload-leader-election
    ├── masterleases
    │   └── 172.17.0.2
    ├── namespaces
    │   ├── default
    │   ├── kube-node-lease
    │   ├── kube-public
    │   └── kube-system
    ├── priorityclasses
    │   ├── system-cluster-critical
    │   └── system-node-critical
    ├── prioritylevelconfigurations
    │   ├── catch-all
    │   ├── exempt
    │   ├── global-default
    │   ├── leader-election
    │   ├── node-high
    │   ├── system
    │   ├── workload-high
    │   └── workload-low
    ├── ranges
    │   ├── serviceips
    │   └── servicenodeports
    └── services
        ├── endpoints
        │   └── default
        │       └── kubernetes
        └── specs
            └── default
                └── kubernetes

18 directories, 64 files

$ docker run --rm -v gitcd-backend:/backend busybox:1 cat /backend/registry/namespaces/kube-system
apiVersion: v1
kind: Namespace
metadata:
  creationTimestamp: "2023-08-14T08:33:54Z"
  labels:
    kubernetes.io/metadata.name: kube-system
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:labels:
          .: {}
          f:kubernetes.io/metadata.name: {}
    manager: kube-apiserver
    operation: Update
    time: "2023-08-14T08:33:54Z"
  name: kube-system
  uid: f0311d5f-eafb-4dad-b3ca-afbc0832d780
spec:
  finalizers:
  - kubernetes
status:
  phase: Active
```

#### Cleanup

```sh
# Stop kube-apiserver and Gitcd containers.
$ make stop-docker-gitcd-kube-apiserver
hack/kube/stop.sh
+ docker stop gitcd kube-apiserver
gitcd
kube-apiserver

# Clean up kube-apiserver and Gitcd container and volumes.
$ make cleanup-docker-gitcd-kube-apiserver
hack/kube/cleanup.sh
+ docker stop kube-apiserver
kube-apiserver
+ docker rm kube-apiserver
kube-apiserver
+ docker stop gitcd
gitcd
+ docker rm gitcd
gitcd
+ docker volume rm kube-certs gitcd-backend
kube-certs
gitcd-backend
```
