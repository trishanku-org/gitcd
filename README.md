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

## Take it for a spin

### Prerequisites

1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
1. [Docker](https://docs.docker.com/engine/install/)

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
$ make docker-run
docker run -it --rm --tmpfs /tmp/trishanku/gitcd:rw,noexec,nosuid,size=65536k --name gitcd "trishanku/gitcd:latest"
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
$ RUN_ARGS=version make docker-run
docker run -it --rm --tmpfs /tmp/trishanku/gitcd:rw,noexec,nosuid,size=65536k --name gitcd "trishanku/gitcd:latest" version
Gitcd: v0.0.1-dev
```

### Serve as ETCD with the backend repo in TMPFS
```sh
$ RUN_ARGS=serve make docker-run
docker run -d --rm --tmpfs /tmp/trishanku/gitcd:rw,noexec,nosuid,size=65536k --name gitcd "trishanku/gitcd:latest" serve
72910d5706b501ea8d24c87ca467b3006b9edb30989733b8562efa2e4e9fd7c9
```

#### Consume

_In a separate shell_.

```sh
# Check ETCD endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status
{"level":"warn","ts":"2022-01-01T11:38:48.900Z","logger":"etcd-client","caller":"v3/retry_interceptor.go:62","msg":"retrying of unary invoker failed","target":"etcd-endpoints://0xc000458700/localhost:2379/","attempt":0,"error":"rpc error: code = Unknown desc = reference 'refs/gitcd/metadata/refs/heads/main' not found"}
Failed to get the status of endpoint http://localhost:2379/ (rpc error: code = Unknown desc = reference 'refs/gitcd/metadata/refs/heads/main' not found)

# Insert a key and value to create a the main branch that is being used as the backend.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport put /a a
OK

# Read all keys.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix /
/a
a

# Check endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table
+------------------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
|        ENDPOINT        | ID | VERSION | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+------------------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
| http://localhost:2379/ |  0 |         |  2.9 kB |      true |      false |         1 |          1 |                  1 |        |
+------------------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+

# List ETCD members.
$ docker run -it --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport member list -w table
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
$ docker volume create gitcd-backend-repo
gitcd-backend-repo

# Serve as ETCD with the backend repo in the volume.
$ DOCKER_RUN_OPTS="-v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd"  RUN_ARGS=serve make docker-run
docker run -d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd "trishanku/gitcd:latest" serve
34560a9ddc640f3268230d950d60430122ac955cf949dda93832222cc799b37e
```

#### Consume

```sh
# Check if the backend repo has been initialized as an empty and bare Git repo neither any data nor any references.
$ docker run --rm -v gitcd-backend-repo:/backend busybox ls -R /backend
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
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport put /1 1
OK

# Read all keys.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix / -w fields
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
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main
commit bd3e502b63d071ab26098be728db6a084ff24045
Author: trishanku <trishanku@heaven.com>
Date:   Sat Jan 1 12:33:53 2022 +0000

    1

# Check the diff for the first commit.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
commit bd3e502b63d071ab26098be728db6a084ff24045
tree 3f258de18b33904bb291ad94cc7d60e4c197fd50
author trishanku <trishanku@heaven.com> 1641040433 +0000
committer trishanku <trishanku@heaven.com> 1641040433 +0000

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
$ docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport txn -w fields <<EOF

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
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix / 
/1
one
/2
2

# Check that the main branch is now advanced by a commit.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main
commit 3998ec995d8b967d0163a4e7107b487d36d0940e
Author: trishanku <trishanku@heaven.com>
Date:   Sat Jan 1 12:35:07 2022 +0000

    2

commit bd3e502b63d071ab26098be728db6a084ff24045
Author: trishanku <trishanku@heaven.com>
Date:   Sat Jan 1 12:33:53 2022 +0000

    1

# Check the diff for the new commit.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
commit 3998ec995d8b967d0163a4e7107b487d36d0940e
tree 904b9644b5f7f0bc10a4d52850994bb58ac1591e
parent bd3e502b63d071ab26098be728db6a084ff24045
author trishanku <trishanku@heaven.com> 1641040507 +0000
committer trishanku <trishanku@heaven.com> 1641040507 +0000

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
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=1 --prefix /
/1
1

# Delete one key and put a new key in one transaction.
$ docker run -i --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport txn -w fields <<EOF
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
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix / 
/1
one
/3/3
33

# Check that the main branch is now advanced by a commit.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main
commit 6a3a357f47565d4db3ad0d970d2a9f04d675023b
Author: trishanku <trishanku@heaven.com>
Date:   Sat Jan 1 12:58:01 2022 +0000

    3

commit 3998ec995d8b967d0163a4e7107b487d36d0940e
Author: trishanku <trishanku@heaven.com>
Date:   Sat Jan 1 12:35:07 2022 +0000

    2

commit bd3e502b63d071ab26098be728db6a084ff24045
Author: trishanku <trishanku@heaven.com>
Date:   Sat Jan 1 12:33:53 2022 +0000

    1

# Check the diff for the new commit.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
commit 6a3a357f47565d4db3ad0d970d2a9f04d675023b
tree be974b5190bf4ca3f70d8474e2b5bbcbfda5156b
parent 3998ec995d8b967d0163a4e7107b487d36d0940e
author trishanku <trishanku@heaven.com> 1641041881 +0000
committer trishanku <trishanku@heaven.com> 1641041881 +0000

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
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=2 --prefix /
/1
one
/2
2

# Read all keys at the previous revision.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=1 --prefix /
/1
1
```

#### Cleanup

```sh
# Stop the Gitcd container.
$ docker stop gitcd
gitcd

# Remove the volume backing the backend Git repo.
$ docker volume rm gitcd-backend-repo
gitcd-backend-repo
```