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
  - [Run kube-apiserver with Gitcd as the backend](#run-kube-apiserver-with-gitcd-as-the-backend)
    - [Cleanup](#cleanup-3)

## Take it for a spin

### Prerequisites

1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
1. [Docker](https://docs.docker.com/engine/install/) 
1. [Kind](https://kind.sigs.k8s.io), a tool for running local Kubernetes clusters using Docker container "nodes".
1. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/#install-with-homebrew-on-macos), the Kubernetes command-line tool.
1. [Caffeinate](https://ss64.com/osx/caffeinate.html) to keep the laptop from going to sleep during some long running steps below.

### Build

```sh
# Clone the Git repo somewhere locally.
$ git clone https://github.com/amshuman-kr/gitcd <somewhere>

# Move to the checked out repo.
$ cd <somewhere>

# Build the docker image.
$ make docker-build
docker build -t "trishanku/gitcd:latest" .
[+] Building 3.8s (19/19) FINISHED
```

### Run help and version commands

```sh
# Print the help text.
$ DOCKER_RUN_OPTS="--rm --name gitcd" make docker-run
docker run --rm --name gitcd "trishanku/gitcd:latest" 
Gitcd - Git as a distributed key-value store.

Usage:
  gitcd [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  init        Init a Git repo for use as a backend.
  serve       Start GRPC service compatible with any ETCD client.
  version     Prints the version of Gitcd.

Flags:
      --debug            Enable debug logging
  -h, --help             help for gitcd
      --verbosity int8   Logging verbosity

Use "gitcd [command] --help" for more information about a command.

# Print Gitcd version.
$ DOCKER_RUN_OPTS="--rm --name gitcd" RUN_ARGS=version make docker-run
docker run --rm --name gitcd "trishanku/gitcd:latest" version
Gitcd: v0.0.1-dev
```

### Serve as ETCD with the backend repo in TMPFS
```sh
$ RUN_ARGS=serve make docker-run
docker run -d --rm --tmpfs /tmp/trishanku/gitcd:rw,noexec,nosuid,size=65536k --name gitcd "trishanku/gitcd:latest" serve
83b5c19b38fb2f300f8a12867ac0af77faaed0e2de1e0b7463ae2e18f8bd738a
```

#### Consume

```sh
# Check ETCD endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table
{"level":"warn","ts":"2022-01-24T14:52:50.313Z","logger":"etcd-client","caller":"v3/retry_interceptor.go:62","msg":"retrying of unary invoker failed","target":"etcd-endpoints://0xc00013aa80/127.0.0.1:2379","attempt":0,"error":"rpc error: code = Unknown desc = reference 'refs/gitcd/metadata/refs/heads/main' not found"}
Failed to get the status of endpoint 127.0.0.1:2379 (rpc error: code = Unknown desc = reference 'refs/gitcd/metadata/refs/heads/main' not found)
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
| 127.0.0.1:2379 |  0 |         |  2.9 kB |      true |      false |         1 |          1 |                  1 |        |
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
docker run -d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd "trishanku/gitcd:latest" serve
cacbcecb5794c3cb3c653978a75c40ecadb391b8878839077749fe4f46e2e995
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
commit 3df9f7d11e0ff4d64154d87ced019bcd577ce7af
Author: trishanku <trishanku@heaven.com>
Date:   Mon Jan 24 14:52:54 2022 +0000

    1

# Check the diff for the first commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
commit 3df9f7d11e0ff4d64154d87ced019bcd577ce7af
tree 3f258de18b33904bb291ad94cc7d60e4c197fd50
author trishanku <trishanku@heaven.com> 1643035974 +0000
committer trishanku <trishanku@heaven.com> 1643035974 +0000

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
commit ee19129f6827d7d49887148e4380043973480e5e
Author: trishanku <trishanku@heaven.com>
Date:   Mon Jan 24 14:52:56 2022 +0000

    2

commit 3df9f7d11e0ff4d64154d87ced019bcd577ce7af
Author: trishanku <trishanku@heaven.com>
Date:   Mon Jan 24 14:52:54 2022 +0000

    1

# Check the diff for the new commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
commit ee19129f6827d7d49887148e4380043973480e5e
tree 904b9644b5f7f0bc10a4d52850994bb58ac1591e
parent 3df9f7d11e0ff4d64154d87ced019bcd577ce7af
author trishanku <trishanku@heaven.com> 1643035976 +0000
committer trishanku <trishanku@heaven.com> 1643035976 +0000

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
commit 59ffb25cc9c63845001ffc0ecfe1e52aa1055140
Author: trishanku <trishanku@heaven.com>
Date:   Mon Jan 24 14:52:59 2022 +0000

    3

commit ee19129f6827d7d49887148e4380043973480e5e
Author: trishanku <trishanku@heaven.com>
Date:   Mon Jan 24 14:52:56 2022 +0000

    2

commit 3df9f7d11e0ff4d64154d87ced019bcd577ce7af
Author: trishanku <trishanku@heaven.com>
Date:   Mon Jan 24 14:52:54 2022 +0000

    1

# Check the diff for the new commit.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git show --pretty=raw main
commit 59ffb25cc9c63845001ffc0ecfe1e52aa1055140
tree be974b5190bf4ca3f70d8474e2b5bbcbfda5156b
parent ee19129f6827d7d49887148e4380043973480e5e
author trishanku <trishanku@heaven.com> 1643035979 +0000
committer trishanku <trishanku@heaven.com> 1643035979 +0000

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
commit a1fb9ff1e4de40337735d07ca0773cfc242ad00f
Merge: f9a8c49c6 a53074542
Author: Piotr Tabor <ptab@google.com>
Date:   Mon Jan 24 12:28:42 2022 +0100

    Merge pull request #13621 from serathius/integration-v2-api
    
    Remove V2 API usage from Integration tests

commit f9a8c49c695b098d66a07948666664ea10d01a82
Merge: 451ea5406 15568f4c0
Author: Piotr Tabor <ptab@google.com>
Date:   Sat Jan 22 16:05:43 2022 +0100

    Merge pull request #13555 from ahrtr/protect_range_sort
    
    Add protection code for Range when the sortTarget is an invalid value

# Init the ETCD metadata for the main branch to prepare it to be served.
# NOTE: This might take a while because the whole commit history of the main branch is traversed to generate the corresponding metadata.
$ DOCKER_RUN_OPTS="--rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=init make docker-run
docker run --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd "trishanku/gitcd:latest" init
{"level":"info","ts":1643036045.3352044,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/tmp/trishanku/gitcd","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1643036200.1303434,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}

# Check that the metadata has been prepared.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log refs/gitcd/metadata/refs/heads/main -n 2
commit 70dc02c8d7c5f364effdb534edcb7021ab89878e
Merge: 174d55a1d f4e073bf1
Author: trishanku <trishanku@heaven.com>
Date:   Mon Jan 24 14:56:40 2022 +0000

    6871

commit f4e073bf1bdc0b6626d7d9aad8b16b23ee9c30c9
Author: trishanku <trishanku@heaven.com>
Date:   Mon Jan 24 14:56:40 2022 +0000

    6874

# Check that the main branch is unmodified.
$ docker run --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 git log main -n 2
commit a1fb9ff1e4de40337735d07ca0773cfc242ad00f
Merge: f9a8c49c6 a53074542
Author: Piotr Tabor <ptab@google.com>
Date:   Mon Jan 24 12:28:42 2022 +0100

    Merge pull request #13621 from serathius/integration-v2-api
    
    Remove V2 API usage from Integration tests

commit f9a8c49c695b098d66a07948666664ea10d01a82
Merge: 451ea5406 15568f4c0
Author: Piotr Tabor <ptab@google.com>
Date:   Sat Jan 22 16:05:43 2022 +0100

    Merge pull request #13555 from ahrtr/protect_range_sort
    
    Add protection code for Range when the sortTarget is an invalid value

# Serve the prepared repo as ETCD.
$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
docker run -d --rm -v gitcd-backend:/tmp/trishanku/gitcd --name gitcd "trishanku/gitcd:latest" serve
8a84e40134040a7b7484fe446be8caeb6dfe73f81b6e2f0cd85f7258c16487de
```

#### Consume

```sh
# Check ETCD endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport endpoint status -w table
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+
|    ENDPOINT    | ID |  VERSION   | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+
| 127.0.0.1:2379 |  0 | v0.0.1-dev |  146 MB |      true |      false |      6871 |       6871 |               6871 |        |
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+

# List main.go keys.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --keys-only --prefix / | grep main.go
/contrib/raftexample/main.go
/etcdctl/main.go
/etcdutl/main.go
/server/etcdmain/main.go
/server/main.go
/tests/functional/cmd/etcd-agent/main.go
/tests/functional/cmd/etcd-proxy/main.go
/tests/functional/cmd/etcd-runner/main.go
/tools/benchmark/main.go
/tools/etcd-dump-db/main.go
/tools/etcd-dump-logs/main.go
/tools/etcd-dump-metrics/main.go

# Read /etcdctl/main.go key.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go | tail -n 20

func main() {
	apiv := os.Getenv(apiEnv)

	// unset apiEnv to avoid side-effect for future env and flag parsing.
	os.Unsetenv(apiEnv)
	if len(apiv) == 0 || apiv == "3" {
		ctlv3.MustStart()
		return
	}

	if apiv == "2" {
		ctlv2.MustStart()
		return
	}

	fmt.Fprintf(os.Stderr, "unsupported API version: %v\n", apiv)
	os.Exit(1)
}


# Read metadata for /etcdctl/main.go key.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go -w fields | grep -e Key -e Revision -e Version | grep -v Value
"Revision" : 6871
"Key" : "/etcdctl/main.go"
"CreateRevision" : 818
"ModRevision" : 6426
"Version" : 36

# Check the difference between the current and previous versions of the value for /etcdctl/main.go key.
$ diff <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get /etcdctl/main.go ) <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd:3 --insecure-transport get --rev=6425 /etcdctl/main.go )
23,24c23,24
< 	"go.etcd.io/etcd/etcdctl/v3/ctlv2"
< 	"go.etcd.io/etcd/etcdctl/v3/ctlv3"
---
> 	"go.etcd.io/etcd/v3/etcdctl/ctlv2"
> 	"go.etcd.io/etcd/v3/etcdctl/ctlv3"
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
$ make start-docker-gitcd-kube-apiserver
hack/kube/start.sh

Starting container etcd-events on port 2379.
etcd-events
2c1377dbf8e5a1d1ae765c45e0b32e9da37693d11ed6b6fe049ae4d2d7ca2fcd
127.0.0.1:2379, 8e9e05c52164694d, 3.5.1, 20 kB, true, false, 2, 4, 4,

Starting container gitcd-main on port 2479.
gitcd-main
Initialized empty Git repository in /backend/.git/
[main (root-commit) a06d42e] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init
{"level":"info","ts":1647778784.2813747,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1647778784.288507,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
9af2d1274dabe663af373094da86459ac76a28905cae617e0f3056bed9954b29
127.0.0.1:2379, 0, v0.0.1-dev, 161 kB, true, false, 1, 1, 1,

Starting container gitcd-nodes on port 2579.
gitcd-nodes
Initialized empty Git repository in /backend/.git/
[main (root-commit) 2cc8fe9] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init
{"level":"info","ts":1647778787.4042866,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1647778787.4113333,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
9b6e02be14b3e5a0c74c7863a5af9f16505d95133cc93146db1d71c4361fcae5
127.0.0.1:2379, 0, v0.0.1-dev, 161 kB, true, false, 1, 1, 1,

Starting container gitcd-leases on port 2679.
gitcd-leases
Initialized empty Git repository in /backend/.git/
[main (root-commit) ed96c19] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init
{"level":"info","ts":1647778790.3922682,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1647778790.400751,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
480dbbfb1a1ba25b4c964201e85ee1181f014921c7ee9acdc49babbc7989601b
127.0.0.1:2379, 0, v0.0.1-dev, 161 kB, true, false, 1, 1, 1,

Starting container gitcd-priorityclasses on port 2779.
gitcd-priorityclasses
Initialized empty Git repository in /backend/.git/
[main (root-commit) 3a29f5b] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init
{"level":"info","ts":1647778793.3959885,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1647778793.4022024,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
7bc0fe4bedcba4761cd7fe66a987717e977e9a42648719215f68a829919b6ba2
127.0.0.1:2379, 0, v0.0.1-dev, 157 kB, true, false, 1, 1, 1,

Starting container gitcd-pods on port 2879.
gitcd-pods
Initialized empty Git repository in /backend/.git/
[main (root-commit) 53fda86] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init
{"level":"info","ts":1647778796.4278045,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1647778796.4346044,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
bee7c521d062a52cfa198a180628417d7c83f8f447158de34a21c869c16d702d
127.0.0.1:2379, 0, v0.0.1-dev, 161 kB, true, false, 1, 1, 1,

Starting container gitcd-configmaps on port 2979.
gitcd-configmaps
Initialized empty Git repository in /backend/.git/
[main (root-commit) 9396826] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init
{"level":"info","ts":1647778799.4809408,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1647778799.4879394,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
7f8d77bb82dc79d523c5c729df8b0b37006b72c2fb95a66ff0ef774fd672e3e7
127.0.0.1:2379, 0, v0.0.1-dev, 161 kB, true, false, 1, 1, 1,

Creating a local Kubernetes cluster using Kind configuration in ./hack/kube/kind-config.yaml.
Creating cluster "trishanku" ...
 ✓ Ensuring node image (kindest/node:v1.21.1) 🖼
 ✓ Preparing nodes 📦
 ✓ Writing configuration 📜
 ✓ Starting control-plane 🕹️
 ✓ Installing CNI 🔌
 ✓ Installing StorageClass 💾
Set kubectl context to "kind-trishanku"
You can now use your cluster with:

kubectl cluster-info --context kind-trishanku

Not sure what to do next? 😅  Check out https://kind.sigs.k8s.io/docs/user/quick-start/

# Check that Kind and Gitcd containers are running.
$ docker ps -n 8
CONTAINER ID   IMAGE                    COMMAND                  CREATED              STATUS          PORTS                                                 NAMES
e7f7b671a623   kindest/node:v1.21.1     "/usr/local/bin/entr…"   38 seconds ago       Up 34 seconds   127.0.0.1:57508->6443/tcp                             trishanku-control-plane
7f8d77bb82dc   trishanku/gitcd:latest   "/gitcd serve --repo…"   41 seconds ago       Up 40 seconds   0.0.0.0:2979->2379/tcp, :::2979->2379/tcp             gitcd-configmaps
bee7c521d062   trishanku/gitcd:latest   "/gitcd serve --repo…"   44 seconds ago       Up 43 seconds   0.0.0.0:2879->2379/tcp, :::2879->2379/tcp             gitcd-pods
7bc0fe4bedcb   trishanku/gitcd:latest   "/gitcd serve --repo…"   47 seconds ago       Up 46 seconds   0.0.0.0:2779->2379/tcp, :::2779->2379/tcp             gitcd-priorityclasses
480dbbfb1a1b   trishanku/gitcd:latest   "/gitcd serve --repo…"   50 seconds ago       Up 49 seconds   0.0.0.0:2679->2379/tcp, :::2679->2379/tcp             gitcd-leases
9b6e02be14b3   trishanku/gitcd:latest   "/gitcd serve --repo…"   53 seconds ago       Up 52 seconds   0.0.0.0:2579->2379/tcp, :::2579->2379/tcp             gitcd-nodes
9af2d1274dab   trishanku/gitcd:latest   "/gitcd serve --repo…"   56 seconds ago       Up 55 seconds   0.0.0.0:2479->2379/tcp, :::2479->2379/tcp             gitcd-main
2c1377dbf8e5   bitnami/etcd:3           "/opt/bitnami/script…"   About a minute ago   Up 59 seconds   0.0.0.0:2379->2379/tcp, :::2379->2379/tcp, 2380/tcp   etcd-events

# Check that the kubeconfig is pointint to the newly setup cluster.
$ kubectl config current-context
kind-trishanku

# Check the Kubernetes cluster information.
$ kubectl cluster-info
Kubernetes control plane is running at https://127.0.0.1:57508
CoreDNS is running at https://127.0.0.1:57508/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.

# List the namespaces in the cluster.
$ kubectl get namespaces
NAME                 STATUS   AGE
default              Active   12s
kube-node-lease      Active   16s
kube-public          Active   16s
kube-system          Active   16s
local-path-storage   Active   5s

# List the nodes in the cluster.
$ kubectl get nodes
NAME                      STATUS     ROLES                  AGE   VERSION
trishanku-control-plane   NotReady   control-plane,master   16s   v1.21.1

# Wait for the node to be Ready.
$ caffeinate -disu sleep 6m

# Check if the node is Ready.
$ kubectl get nodes
NAME                      STATUS     ROLES                  AGE   VERSION
trishanku-control-plane   Ready      control-plane,master   6m    v1.21.1

# List all the pods in the cluster across all namespaces.
$ kubectl get pods --all-namespaces
NAMESPACE     NAME                                              READY   STATUS    RESTARTS   AGE
kube-system   kindnet-mqtkh                                     1/1     Running   0          103s
kube-system   kube-apiserver-trishanku-control-plane            1/1     Running   1          119s
kube-system   kube-controller-manager-trishanku-control-plane   1/1     Running   0          112s
kube-system   kube-proxy-f8tmw                                  1/1     Running   0          103s
kube-system   kube-scheduler-trishanku-control-plane            1/1     Running   0          55s

# Run a pod to say hello.
$ kubectl run -i -t hello --image=busybox:1 --restart=Never --rm echo  'Hello, World!'
Hello, World!
pod "hello" deleted

# Inspect the Kubernetes content in the backend Git repo.
$ echo "git reset --hard && git checkout refs/gitcd/metadata/refs/heads/main && git checkout main" | \
    docker run -i --rm -v gitcd-nodes:/backend -w /backend bitnami/git:2 sh
HEAD is now at e086049 15
Note: switching to 'refs/gitcd/metadata/refs/heads/main'.

You are in 'detached HEAD' state. You can look around, make experimental
changes and commit them, and you can discard any commits you make in this
state without impacting any branches by switching back to a branch.

If you want to create a new branch to retain commits you create, you may
do so (now or later) by using -c with the switch command. Example:

  git switch -c <new-branch-name>

Or undo this operation with:

  git switch -

Turn off this advice by setting config variable advice.detachedHead to false

HEAD is now at 1cb9f0d 15
Previous HEAD position was 1cb9f0d 15
Switched to branch 'main'

docker run --rm -v gitcd-nodes:/backend busybox:1 cat /backend/registry/minions/trishanku-control-plane
apiVersion: v1
kind: Node
metadata:
  annotations:
    kubeadm.alpha.kubernetes.io/cri-socket: unix:///run/containerd/containerd.sock
    node.alpha.kubernetes.io/ttl: "0"
    volumes.kubernetes.io/controller-managed-attach-detach: "true"
  creationTimestamp: "2022-03-20T12:20:26Z"
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
    time: "2022-03-20T12:20:32Z"
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
    manager: kube-controller-manager
    operation: Update
    time: "2022-03-20T12:26:38Z"
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
    time: "2022-03-20T12:26:39Z"
  name: trishanku-control-plane
  uid: 64de4fcb-da69-4495-b281-6d04e90d2fc7
spec:
  podCIDR: 10.244.0.0/24
  podCIDRs:
  - 10.244.0.0/24
  providerID: kind://docker/trishanku/trishanku-control-plane
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
  - lastHeartbeatTime: "2022-03-20T12:26:39Z"
    lastTransitionTime: "2022-03-20T12:26:39Z"
    message: kubelet has sufficient memory available
    reason: KubeletHasSufficientMemory
    status: "False"
    type: MemoryPressure
  - lastHeartbeatTime: "2022-03-20T12:26:39Z"
    lastTransitionTime: "2022-03-20T12:26:39Z"
    message: kubelet has no disk pressure
    reason: KubeletHasNoDiskPressure
    status: "False"
    type: DiskPressure
  - lastHeartbeatTime: "2022-03-20T12:26:39Z"
    lastTransitionTime: "2022-03-20T12:26:39Z"
    message: kubelet has sufficient PID available
    reason: KubeletHasSufficientPID
    status: "False"
    type: PIDPressure
  - lastHeartbeatTime: "2022-03-20T12:26:39Z"
    lastTransitionTime: "2022-03-20T12:26:39Z"
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
    bootID: c4627ea5-610f-49ac-933c-218c8c0eaa20
    containerRuntimeVersion: containerd://1.5.2
    kernelVersion: 5.10.47-linuxkit
    kubeProxyVersion: v1.21.1
    kubeletVersion: v1.21.1
    machineID: 52e68fbe70db45a697a50fb38ee00e69
    operatingSystem: linux
    osImage: Ubuntu 21.04
    systemUUID: 0b00a417-b7af-4451-a5c4-fdc183a9b376

# Check the difference between the content in the backend git repo and the output from kubectl (there might have been more updates in the meantime).
diff -u <( docker run --rm -v gitcd-nodes:/backend busybox:1 cat /backend/registry/minions/trishanku-control-plane ) <( kubectl get node trishanku-control-plane -oyaml )
--- /dev/fd/63	2022-03-20 17:56:53.000000000 +0530
+++ /dev/fd/62	2022-03-20 17:56:53.000000000 +0530
@@ -15,80 +15,8 @@
     node-role.kubernetes.io/control-plane: ""
     node-role.kubernetes.io/master: ""
     node.kubernetes.io/exclude-from-external-load-balancers: ""
-  managedFields:
-  - apiVersion: v1
-    fieldsType: FieldsV1
-    fieldsV1:
-      f:metadata:
-        f:annotations:
-          f:kubeadm.alpha.kubernetes.io/cri-socket: {}
-        f:labels:
-          f:node-role.kubernetes.io/control-plane: {}
-          f:node-role.kubernetes.io/master: {}
-          f:node.kubernetes.io/exclude-from-external-load-balancers: {}
-    manager: kubeadm
-    operation: Update
-    time: "2022-03-20T12:20:32Z"
-  - apiVersion: v1
-    fieldsType: FieldsV1
-    fieldsV1:
-      f:metadata:
-        f:annotations:
-          f:node.alpha.kubernetes.io/ttl: {}
-      f:spec:
-        f:podCIDR: {}
-        f:podCIDRs:
-          .: {}
-          v:"10.244.0.0/24": {}
-    manager: kube-controller-manager
-    operation: Update
-    time: "2022-03-20T12:26:38Z"
-  - apiVersion: v1
-    fieldsType: FieldsV1
-    fieldsV1:
-      f:metadata:
-        f:annotations:
-          .: {}
-          f:volumes.kubernetes.io/controller-managed-attach-detach: {}
-        f:labels:
-          .: {}
-          f:beta.kubernetes.io/arch: {}
-          f:beta.kubernetes.io/os: {}
-          f:kubernetes.io/arch: {}
-          f:kubernetes.io/hostname: {}
-          f:kubernetes.io/os: {}
-      f:spec:
-        f:providerID: {}
-      f:status:
-        f:conditions:
-          k:{"type":"DiskPressure"}:
-            f:lastHeartbeatTime: {}
-            f:lastTransitionTime: {}
-            f:message: {}
-            f:reason: {}
-            f:status: {}
-          k:{"type":"MemoryPressure"}:
-            f:lastHeartbeatTime: {}
-            f:lastTransitionTime: {}
-            f:message: {}
-            f:reason: {}
-            f:status: {}
-          k:{"type":"PIDPressure"}:
-            f:lastHeartbeatTime: {}
-            f:lastTransitionTime: {}
-            f:message: {}
-            f:reason: {}
-            f:status: {}
-          k:{"type":"Ready"}:
-            f:lastHeartbeatTime: {}
-            f:lastTransitionTime: {}
-            f:message: {}
-            f:reason: {}
-            f:status: {}
-    manager: kubelet
-    operation: Update
-    time: "2022-03-20T12:26:39Z"
   name: trishanku-control-plane
+  resourceVersion: "15"
   uid: 64de4fcb-da69-4495-b281-6d04e90d2fc7
 spec:
   podCIDR: 10.244.0.0/24

```

#### Cleanup

```sh
# Stop kube-apiserver and Gitcd containers.
$ make stop-docker-gitcd-kube-apiserver
hack/kube/stop.sh
+ kind delete cluster --name trishanku
Deleting cluster "trishanku" ...
+ docker stop gitcd-main etcd-events gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
gitcd-main
etcd-events
gitcd-nodes
gitcd-leases
gitcd-priorityclasses
gitcd-pods
gitcd-configmaps

# Clean up kube-apiserver and Gitcd container and volumes.
$ make cleanup-docker-gitcd-kube-apiserver
hack/kube/cleanup.sh
+ kind delete cluster --name trishanku
Deleting cluster "trishanku" ...
+ stop_containers gitcd-main etcd-events gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
+ for container_name in '"$@"'
+ docker stop gitcd-main
gitcd-main
+ docker rm gitcd-main
gitcd-main
+ for container_name in '"$@"'
+ docker stop etcd-events
etcd-events
+ docker rm etcd-events
etcd-events
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
+ docker volume rm gitcd-main etcd-events gitcd-nodes gitcd-leases gitcd-priorityclasses gitcd-pods gitcd-configmaps
gitcd-main
etcd-events
gitcd-nodes
gitcd-leases
gitcd-priorityclasses
gitcd-pods
gitcd-configmaps
```