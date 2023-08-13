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
