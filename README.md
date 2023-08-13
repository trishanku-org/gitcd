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
  - [Run a local Kubernetes cluster with Gitcd as the backend](#run-a-local-kubernetes-cluster-with-gitcd-as-the-backend)
    - [Note](#note)
    - [Cleanup](#cleanup-3)

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

### Run a local Kubernetes cluster with Gitcd as the backend

#### Note

The following steps will start a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io).
Please make sure that there are no other local Kubernetes clusters running (e.g. docker-desktop).
These steps also start containers that listen on ports like `2379`, `2479`, `2579`, `2679`, `2779`, `2879`, `2979` and `6443`.
Please ensure that these ports are available before running these steps.

```sh
# Start a local Kind Kubernetes cluster with Gitcd as the backend.
$ make start-docker-gitcd-kind
hack/kubernetes/kind/start.sh

Starting container etcd-events on port 2379.
etcd-events
8c43c409f6071e9659a7ae7d28ab618eabee94f464a942591c51114a1027cb85
127.0.0.1:2379, 8e9e05c52164694d, 3.5.1, 20 kB, true, false, 2, 4, 4, 
gitcd-main
Initialized empty Git repository in /backend/.git/
[main (root-commit) f813228] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init

Starting container gitcd-main main=http://0.0.0.0:2479.
{"level":"info","ts":1657704704.3125026,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefName":"refs/gitcd/metadata/main","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1657704704.3186738,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
1365849800f73770c87b886f2f1c4c8952baef9c4a7a9c74fcdc88d0cf4cc5cb
http://127.0.0.1:2479, 0, v0.0.1-dev, 152 kB, true, false, 1, 1, 1, 
gitcd-nodes
Initialized empty Git repository in /backend/.git/
[main (root-commit) d4a3400] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init

Starting container gitcd-nodes nodes=http://0.0.0.0:2579.
{"level":"info","ts":1657704708.097126,"logger":"init.refs/heads/nodes","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/nodes","MetadataRefName":"refs/gitcd/metadata/nodes","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1657704708.1033726,"logger":"init.refs/heads/nodes","caller":"cmd/init.go:91","msg":"Initialized successfully"}
e73fabbb8a0c83a3b9dd0c5445264208db56048a074a743c02bedc3a4b79ae26
http://127.0.0.1:2579, 0, v0.0.1-dev, 153 kB, true, false, 1, 1, 1, 
gitcd-leases
Initialized empty Git repository in /backend/.git/
[main (root-commit) bf290f7] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init

Starting container gitcd-leases leases=http://0.0.0.0:2679.
{"level":"info","ts":1657704712.1153524,"logger":"init.refs/heads/leases","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/leases","MetadataRefName":"refs/gitcd/metadata/leases","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1657704712.1249726,"logger":"init.refs/heads/leases","caller":"cmd/init.go:91","msg":"Initialized successfully"}
e9df3dcd0a276e291ffc4cdee639b345cc08642a43941b3f6684f34c6943d9fa
http://127.0.0.1:2679, 0, v0.0.1-dev, 153 kB, true, false, 1, 1, 1, 
gitcd-priorityclasses
Initialized empty Git repository in /backend/.git/
[main (root-commit) abde8f6] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init

Starting container gitcd-priorityclasses priorityclasses=http://0.0.0.0:2779.
{"level":"info","ts":1657704716.4430547,"logger":"init.refs/heads/priorityclasses","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/priorityclasses","MetadataRefName":"refs/gitcd/metadata/priorityclasses","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1657704716.4534853,"logger":"init.refs/heads/priorityclasses","caller":"cmd/init.go:91","msg":"Initialized successfully"}
7d022e425e0539931d7ccaee4282988f11515b8e0c4bdea62b2bb01884ea22a9
http://127.0.0.1:2779, 0, v0.0.1-dev, 153 kB, true, false, 1, 1, 1, 
gitcd-pods
Initialized empty Git repository in /backend/.git/
[main (root-commit) 71c400f] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init

Starting container gitcd-pods pods=http://0.0.0.0:2879.
{"level":"info","ts":1657704720.4108195,"logger":"init.refs/heads/pods","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/pods","MetadataRefName":"refs/gitcd/metadata/pods","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1657704720.4171014,"logger":"init.refs/heads/pods","caller":"cmd/init.go:91","msg":"Initialized successfully"}
1d375b5db35afdb534093dd3fc9bc76e972cac915894ad78c5f8fbe7e1238f01
http://127.0.0.1:2879, 0, v0.0.1-dev, 153 kB, true, false, 1, 1, 1, 
gitcd-configmaps
Initialized empty Git repository in /backend/.git/
[main (root-commit) 792846e] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init

Starting container gitcd-configmaps configmaps=http://0.0.0.0:2979.
{"level":"info","ts":1657704724.3323298,"logger":"init.refs/heads/configmaps","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/configmaps","MetadataRefName":"refs/gitcd/metadata/configmaps","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1657704724.3399482,"logger":"init.refs/heads/configmaps","caller":"cmd/init.go:91","msg":"Initialized successfully"}
9dded473170cd653a3f4de60879cd0bd2cd6dc24493991a1e2fee523e5e4ce7e
http://127.0.0.1:2979, 0, v0.0.1-dev, 153 kB, true, false, 1, 1, 1, 

Creating a local Kubernetes cluster using Kind configuration in ./hack/kubernetes/kind/kind-config.yaml.
Creating cluster "trishanku" ...
 • Ensuring node image (kindest/node:v1.21.1) 🖼  ...
 ✓ Ensuring node image (kindest/node:v1.21.1) 🖼
 • Preparing nodes 📦   ...
 ✓ Preparing nodes 📦 
 • Writing configuration 📜  ...
 ✓ Writing configuration 📜
 • Starting control-plane 🕹️  ...
 ✓ Starting control-plane 🕹️
 • Installing CNI 🔌  ...
 ✓ Installing CNI 🔌
 • Installing StorageClass 💾  ...
 ✓ Installing StorageClass 💾
Set kubectl context to "kind-trishanku"
You can now use your cluster with:

kubectl cluster-info --context kind-trishanku

Have a nice day! 👋

# Check that Kind and Gitcd containers are running.
$ docker ps -n 8
CONTAINER ID   IMAGE                    COMMAND                  CREATED              STATUS              PORTS                                                 NAMES
4ed0b902e6c9   kindest/node:v1.21.1     "/usr/local/bin/entr…"   41 seconds ago       Up 37 seconds       127.0.0.1:63651->6443/tcp                             trishanku-control-plane
9dded473170c   trishanku/gitcd:latest   "/gitcd serve --repo…"   44 seconds ago       Up 43 seconds       0.0.0.0:2979->2979/tcp, :::2979->2979/tcp             gitcd-configmaps
1d375b5db35a   trishanku/gitcd:latest   "/gitcd serve --repo…"   48 seconds ago       Up 47 seconds       0.0.0.0:2879->2879/tcp, :::2879->2879/tcp             gitcd-pods
7d022e425e05   trishanku/gitcd:latest   "/gitcd serve --repo…"   52 seconds ago       Up 50 seconds       0.0.0.0:2779->2779/tcp, :::2779->2779/tcp             gitcd-priorityclasses
e9df3dcd0a27   trishanku/gitcd:latest   "/gitcd serve --repo…"   56 seconds ago       Up 55 seconds       0.0.0.0:2679->2679/tcp, :::2679->2679/tcp             gitcd-leases
e73fabbb8a0c   trishanku/gitcd:latest   "/gitcd serve --repo…"   About a minute ago   Up 59 seconds       0.0.0.0:2579->2579/tcp, :::2579->2579/tcp             gitcd-nodes
1365849800f7   trishanku/gitcd:latest   "/gitcd serve --repo…"   About a minute ago   Up About a minute   0.0.0.0:2479->2479/tcp, :::2479->2479/tcp             gitcd-main
8c43c409f607   bitnami/etcd:3           "/opt/bitnami/script…"   About a minute ago   Up About a minute   0.0.0.0:2379->2379/tcp, :::2379->2379/tcp, 2380/tcp   etcd-events

# Check that the kubeconfig is pointint to the newly setup cluster.
$ kubectl config current-context
kind-trishanku

# Check the Kubernetes cluster information.
$ kubectl cluster-info
[0;32mKubernetes control plane[0m is running at [0;33mhttps://127.0.0.1:63651[0m
[0;32mCoreDNS[0m is running at [0;33mhttps://127.0.0.1:63651/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy[0m

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.

# List the namespaces in the cluster.
$ kubectl get namespaces
NAME                 STATUS   AGE
default              Active   13s
kube-node-lease      Active   17s
kube-public          Active   17s
kube-system          Active   17s
local-path-storage   Active   5s

# List the nodes in the cluster.
$ kubectl get nodes
NAME                      STATUS     ROLES                  AGE   VERSION
trishanku-control-plane   NotReady   control-plane,master   17s   v1.21.1

# Wait for the node to be Ready.
$ caffeinate -disu sleep 6m

# Check if the node is Ready.
$ kubectl get nodes
NAME                      STATUS   ROLES                  AGE     VERSION
trishanku-control-plane   Ready    control-plane,master   6m17s   v1.21.1

# List all the pods in the cluster across all namespaces.
$ kubectl get pods --all-namespaces
NAMESPACE     NAME                                              READY   STATUS    RESTARTS   AGE
kube-system   kindnet-4mtsm                                     1/1     Running   0          103s
kube-system   kube-apiserver-trishanku-control-plane            1/1     Running   1          6m14s
kube-system   kube-controller-manager-trishanku-control-plane   1/1     Running   0          6m4s
kube-system   kube-proxy-jtgp2                                  1/1     Running   0          103s
kube-system   kube-scheduler-trishanku-control-plane            1/1     Running   0          6m13s

# Run a pod to say hello.
$ kubectl run -i -t hello --image=busybox:1 --restart=Never --rm echo  'Hello, World!'
Hellow, World!
pod "hello" deleted

# Inspect the Kubernetes content in the backend Git repo.
$ echo "git reset --hard && git checkout refs/gitcd/metadata/nodes && git checkout nodes" | \
    docker run -i --rm -v gitcd-nodes:/backend -w /backend bitnami/git:2 sh
HEAD is now at d4a3400 init
Note: switching to 'refs/gitcd/metadata/nodes'.

You are in 'detached HEAD' state. You can look around, make experimental
changes and commit them, and you can discard any commits you make in this
state without impacting any branches by switching back to a branch.

If you want to create a new branch to retain commits you create, you may
do so (now or later) by using -c with the switch command. Example:

  git switch -c <new-branch-name>

Or undo this operation with:

  git switch -

Turn off this advice by setting config variable advice.detachedHead to false

HEAD is now at eb68381 15
Previous HEAD position was eb68381 15
Switched to branch 'nodes'

$ docker run --rm -v gitcd-nodes:/backend busybox:1 cat /backend/registry/minions/trishanku-control-plane
apiVersion: v1
kind: Node
metadata:
  annotations:
    kubeadm.alpha.kubernetes.io/cri-socket: unix:///run/containerd/containerd.sock
    node.alpha.kubernetes.io/ttl: "0"
    volumes.kubernetes.io/controller-managed-attach-detach: "true"
  creationTimestamp: "2022-07-13T09:32:33Z"
  labels:
    beta.kubernetes.io/arch: amd64
    beta.kubernetes.io/os: linux
    kubernetes.io/arch: amd64
    kubernetes.io/hostname: trishanku-control-plane
    kubernetes.io/os: linux
    node-role.kubernetes.io/control-plane: ""
    node-role.kubernetes.io/master: ""
    node.kubernetes.io/exclude-from-external-load-balancers: ""
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:annotations:
          f:kubeadm.alpha.kubernetes.io/cri-socket: {}
        f:labels:
          f:node-role.kubernetes.io/control-plane: {}
          f:node-role.kubernetes.io/master: {}
          f:node.kubernetes.io/exclude-from-external-load-balancers: {}
    manager: kubeadm
    operation: Update
    time: "2022-07-13T09:32:39Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:annotations:
          f:node.alpha.kubernetes.io/ttl: {}
      f:spec:
        f:podCIDR: {}
        f:podCIDRs:
          .: {}
          v:"10.244.0.0/24": {}
        f:taints: {}
    manager: kube-controller-manager
    operation: Update
    time: "2022-07-13T09:39:35Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:annotations:
          .: {}
          f:volumes.kubernetes.io/controller-managed-attach-detach: {}
        f:labels:
          .: {}
          f:beta.kubernetes.io/arch: {}
          f:beta.kubernetes.io/os: {}
          f:kubernetes.io/arch: {}
          f:kubernetes.io/hostname: {}
          f:kubernetes.io/os: {}
      f:spec:
        f:providerID: {}
      f:status:
        f:conditions:
          k:{"type":"DiskPressure"}:
            f:lastHeartbeatTime: {}
            f:lastTransitionTime: {}
            f:message: {}
            f:reason: {}
            f:status: {}
          k:{"type":"MemoryPressure"}:
            f:lastHeartbeatTime: {}
            f:lastTransitionTime: {}
            f:message: {}
            f:reason: {}
            f:status: {}
          k:{"type":"PIDPressure"}:
            f:lastHeartbeatTime: {}
            f:lastTransitionTime: {}
            f:message: {}
            f:reason: {}
            f:status: {}
          k:{"type":"Ready"}:
            f:lastHeartbeatTime: {}
            f:lastTransitionTime: {}
            f:message: {}
            f:reason: {}
            f:status: {}
    manager: kubelet
    operation: Update
    time: "2022-07-13T09:39:36Z"
  name: trishanku-control-plane
  uid: 8d16a776-1a6c-4156-8a40-de364224bc2b
spec:
  podCIDR: 10.244.0.0/24
  podCIDRs:
  - 10.244.0.0/24
  providerID: kind://docker/trishanku/trishanku-control-plane
  taints:
  - effect: NoSchedule
    key: node.kubernetes.io/unreachable
    timeAdded: "2022-07-13T09:39:35Z"
status:
  addresses:
  - address: 172.18.0.2
    type: InternalIP
  - address: trishanku-control-plane
    type: Hostname
  allocatable:
    cpu: "6"
    ephemeral-storage: 61255492Ki
    hugepages-1Gi: "0"
    hugepages-2Mi: "0"
    memory: 2032964Ki
    pods: "110"
  capacity:
    cpu: "6"
    ephemeral-storage: 61255492Ki
    hugepages-1Gi: "0"
    hugepages-2Mi: "0"
    memory: 2032964Ki
    pods: "110"
  conditions:
  - lastHeartbeatTime: "2022-07-13T09:39:36Z"
    lastTransitionTime: "2022-07-13T09:39:36Z"
    message: kubelet has sufficient memory available
    reason: KubeletHasSufficientMemory
    status: "False"
    type: MemoryPressure
  - lastHeartbeatTime: "2022-07-13T09:39:36Z"
    lastTransitionTime: "2022-07-13T09:39:36Z"
    message: kubelet has no disk pressure
    reason: KubeletHasNoDiskPressure
    status: "False"
    type: DiskPressure
  - lastHeartbeatTime: "2022-07-13T09:39:36Z"
    lastTransitionTime: "2022-07-13T09:39:36Z"
    message: kubelet has sufficient PID available
    reason: KubeletHasSufficientPID
    status: "False"
    type: PIDPressure
  - lastHeartbeatTime: "2022-07-13T09:39:36Z"
    lastTransitionTime: "2022-07-13T09:39:36Z"
    message: kubelet is posting ready status
    reason: KubeletReady
    status: "True"
    type: Ready
  daemonEndpoints:
    kubeletEndpoint:
      Port: 10250
  images:
  - names:
    - k8s.gcr.io/kube-proxy:v1.21.1
    sizeBytes: 132714699
  - names:
    - k8s.gcr.io/kube-apiserver:v1.21.1
    sizeBytes: 126834637
  - names:
    - k8s.gcr.io/kube-controller-manager:v1.21.1
    sizeBytes: 121042741
  - names:
    - k8s.gcr.io/etcd:3.4.13-0
    sizeBytes: 86742272
  - names:
    - docker.io/kindest/kindnetd:v20210326-1e038dc5
    sizeBytes: 53960776
  - names:
    - k8s.gcr.io/kube-scheduler:v1.21.1
    sizeBytes: 51865396
  - names:
    - k8s.gcr.io/build-image/debian-base:v2.1.0
    sizeBytes: 21086532
  - names:
    - docker.io/rancher/local-path-provisioner:v0.0.14
    sizeBytes: 13367922
  - names:
    - k8s.gcr.io/coredns/coredns:v1.8.0
    sizeBytes: 12945155
  - names:
    - k8s.gcr.io/pause:3.5
    sizeBytes: 301416
  nodeInfo:
    architecture: amd64
    bootID: 6ab525ed-1014-4e3e-9e85-272245653da2
    containerRuntimeVersion: containerd://1.5.2
    kernelVersion: 5.10.47-linuxkit
    kubeProxyVersion: v1.21.1
    kubeletVersion: v1.21.1
    machineID: f338bc235ee44331951056d3bae2e93c
    operatingSystem: linux
    osImage: Ubuntu 21.04
    systemUUID: 609dad2a-cca7-44bf-8a75-deba15f54ddf

# Check the difference between the content in the backend git repo and the output from kubectl (there might have been more updates in the meantime).
$ diff -u <( docker run --rm -v gitcd-nodes:/backend busybox:1 cat /backend/registry/minions/trishanku-control-plane ) <( kubectl get node trishanku-control-plane --show-managed-fields=true -oyaml )
--- /dev/fd/63	2022-07-13 15:09:52.000000000 +0530
+++ /dev/fd/62	2022-07-13 15:09:52.000000000 +0530
@@ -90,6 +90,7 @@
     operation: Update
     time: "2022-07-13T09:39:36Z"
   name: trishanku-control-plane
+  resourceVersion: "15"
   uid: 8d16a776-1a6c-4156-8a40-de364224bc2b
 spec:
   podCIDR: 10.244.0.0/24

```

#### Cleanup

```sh
# Stop kube-apiserver and Gitcd containers.
$ make stop-docker-gitcd-kind
hack/kubernetes/kind/stop.sh
+ kind delete cluster --name trishanku
Deleting cluster "trishanku" ...
+ docker stop etcd-events gitcd-main gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
etcd-events
gitcd-main
gitcd-nodes
gitcd-leases
gitcd-priorityclasses
gitcd-pods
gitcd-configmaps

# Clean up kube-apiserver and Gitcd container and volumes.
$ make cleanup-docker-gitcd-kind
hack/kubernetes/kind/cleanup.sh
+ stop_containers etcd-events gitcd-main gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
+ for container_name in '"$@"'
+ docker stop etcd-events
etcd-events
+ docker rm etcd-events
etcd-events
+ for container_name in '"$@"'
+ docker stop gitcd-main
gitcd-main
+ docker rm gitcd-main
gitcd-main
+ for container_name in '"$@"'
+ docker stop gitcd-nodes
gitcd-nodes
+ docker rm gitcd-nodes
gitcd-nodes
+ for container_name in '"$@"'
+ docker stop gitcd-leases
gitcd-leases
+ docker rm gitcd-leases
gitcd-leases
+ for container_name in '"$@"'
+ docker stop gitcd-priorityclasses
gitcd-priorityclasses
+ docker rm gitcd-priorityclasses
gitcd-priorityclasses
+ for container_name in '"$@"'
+ docker stop gitcd-pods
gitcd-pods
+ docker rm gitcd-pods
gitcd-pods
+ for container_name in '"$@"'
+ docker stop gitcd-configmaps
gitcd-configmaps
+ docker rm gitcd-configmaps
gitcd-configmaps
+ docker volume rm etcd-events gitcd-main gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
etcd-events
gitcd-main
gitcd-nodes
gitcd-leases
gitcd-priorityclasses
gitcd-pods
gitcd-configmaps
```
