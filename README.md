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
3986f63eda63ab685e061910ed8d4d47963e10286ca9af09c53b1bde2907ed84
```

#### Consume

```sh
# Check ETCD endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table
{"level":"warn","ts":"2022-01-14T15:00:10.482Z","logger":"etcd-client","caller":"v3/retry_interceptor.go:62","msg":"retrying of unary invoker failed","target":"etcd-endpoints://0xc0001a2000/127.0.0.1:2379","attempt":0,"error":"rpc error: code = Unknown desc = reference 'refs/gitcd/metadata/refs/heads/main' not found"}
Failed to get the status of endpoint 127.0.0.1:2379 (rpc error: code = Unknown desc = reference 'refs/gitcd/metadata/refs/heads/main' not found)
+----------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
| ENDPOINT | ID | VERSION | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+----------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
+----------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+

# Insert a key and value to create a the main branch that is being used as the backend.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport put /a a
OK

# Read all keys.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --prefix /
/a
a

# Check endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table
+----------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
|    ENDPOINT    | ID | VERSION | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+----------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
| 127.0.0.1:2379 |  0 |         |  2.9 kB |      true |      false |         1 |          1 |                  1 |        |
+----------------+----+---------+---------+-----------+------------+-----------+------------+--------------------+--------+

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
$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
docker run -d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd "trishanku/gitcd:latest" serve
d794859d09d38c37048d9bf9db350cb8fa1fff0cccc56230d76e4f02b16305a1
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
commit 8311d7cb8e89147dd89e67dba8832d5b7bd11721
Author: trishanku <trishanku@heaven.com>
Date:   Fri Jan 14 15:00:14 2022 +0000

    1

# Check the diff for the first commit.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
commit 8311d7cb8e89147dd89e67dba8832d5b7bd11721
tree 3f258de18b33904bb291ad94cc7d60e4c197fd50
author trishanku <trishanku@heaven.com> 1642172414 +0000
committer trishanku <trishanku@heaven.com> 1642172414 +0000

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
commit 4947374283cfa1a2761f892bb265c2109b6f1f3d
Author: trishanku <trishanku@heaven.com>
Date:   Fri Jan 14 15:00:17 2022 +0000

    2

commit 8311d7cb8e89147dd89e67dba8832d5b7bd11721
Author: trishanku <trishanku@heaven.com>
Date:   Fri Jan 14 15:00:14 2022 +0000

    1

# Check the diff for the new commit.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
commit 4947374283cfa1a2761f892bb265c2109b6f1f3d
tree 904b9644b5f7f0bc10a4d52850994bb58ac1591e
parent 8311d7cb8e89147dd89e67dba8832d5b7bd11721
author trishanku <trishanku@heaven.com> 1642172417 +0000
committer trishanku <trishanku@heaven.com> 1642172417 +0000

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
commit 7c6378f63162f5c848269506c212113f9f2e7afc
Author: trishanku <trishanku@heaven.com>
Date:   Fri Jan 14 15:00:20 2022 +0000

    3

commit 4947374283cfa1a2761f892bb265c2109b6f1f3d
Author: trishanku <trishanku@heaven.com>
Date:   Fri Jan 14 15:00:17 2022 +0000

    2

commit 8311d7cb8e89147dd89e67dba8832d5b7bd11721
Author: trishanku <trishanku@heaven.com>
Date:   Fri Jan 14 15:00:14 2022 +0000

    1

# Check the diff for the new commit.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git show --pretty=raw main
commit 7c6378f63162f5c848269506c212113f9f2e7afc
tree be974b5190bf4ca3f70d8474e2b5bbcbfda5156b
parent 4947374283cfa1a2761f892bb265c2109b6f1f3d
author trishanku <trishanku@heaven.com> 1642172420 +0000
committer trishanku <trishanku@heaven.com> 1642172420 +0000

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

# Read all keys at the first revision.
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

# Serve ETCD from an existing Git repo

```sh
# Create a volume to store the backend repo.
$ docker volume create gitcd-backend-repo
gitcd-backend-repo

# Clone a Git repo into the volume to prepare it to be served.
# NOTE: This might take a while.
$ docker run --rm -v gitcd-backend-repo:/backend bitnami/git git clone https://github.com/etcd-io/etcd /backend
Cloning into '/backend'...

# Check that the repo got cloned properly.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main -n 2
commit f75549d53b93f44ff43dbd4c9279612c974edcd3
Merge: f70c14128 77bf0a5a9
Author: Piotr Tabor <ptab@google.com>
Date:   Fri Jan 14 14:54:28 2022 +0100

    Merge pull request #13571 from yank1/update-cobra-version
    
    Update Cobra version to 1.2.1

commit f70c14128dbea3e87bd95794abd2c4752c9776c8
Merge: 68fa5dcf9 357006172
Author: Piotr Tabor <ptab@google.com>
Date:   Fri Jan 14 14:54:00 2022 +0100

    Merge pull request #13597 from mhoffm-aiven/mhoffm-add-forgotten-method-to-printerrpc
    
    ctlv3: add forgotten member promote method to printerRPC

# Init the ETCD metadata for the main branch to prepare it to be served.
# NOTE: This might take a while because the whole commit history of the main branch is traversed to generate the corresponding metadata.
$ DOCKER_RUN_OPTS="--rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=init make docker-run
docker run --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd "trishanku/gitcd:latest" init
{"level":"info","ts":1642172466.191522,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1642172631.6238172,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}

# Check that the metadata has been prepared.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log refs/gitcd/metadata/refs/heads/main -n 2
commit c80d85d73a42cd61326835dedafbecec2f33461c
Merge: a4e7e3863 d55814a50
Author: trishanku <trishanku@heaven.com>
Date:   Fri Jan 14 15:03:51 2022 +0000

    6847

commit a4e7e38630efe4a3debc20fe68f3b408cf7905a1
Merge: 8fbae53e1 643d67c16
Author: trishanku <trishanku@heaven.com>
Date:   Fri Jan 14 15:03:51 2022 +0000

    6846

# Check that the main branch is unmodified.
$ docker run --rm -v gitcd-backend-repo:/backend -w /backend bitnami/git git log main -n 2
commit f75549d53b93f44ff43dbd4c9279612c974edcd3
Merge: f70c14128 77bf0a5a9
Author: Piotr Tabor <ptab@google.com>
Date:   Fri Jan 14 14:54:28 2022 +0100

    Merge pull request #13571 from yank1/update-cobra-version
    
    Update Cobra version to 1.2.1

commit f70c14128dbea3e87bd95794abd2c4752c9776c8
Merge: 68fa5dcf9 357006172
Author: Piotr Tabor <ptab@google.com>
Date:   Fri Jan 14 14:54:00 2022 +0100

    Merge pull request #13597 from mhoffm-aiven/mhoffm-add-forgotten-method-to-printerrpc
    
    ctlv3: add forgotten member promote method to printerRPC

# Serve the prepared repo as ETCD.
$ DOCKER_RUN_OPTS="-d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd" RUN_ARGS=serve make docker-run
docker run -d --rm -v gitcd-backend-repo:/tmp/trishanku/gitcd --name gitcd "trishanku/gitcd:latest" serve
06eff15a6aa9b323854086047f4ebdc69387132047b1a6d7ede1e13b967095ac
```

#### Consume

```sh
# Check ETCD endpoint status.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport endpoint status -w table
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+
|    ENDPOINT    | ID |  VERSION   | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+
| 127.0.0.1:2379 |  0 | v0.0.1-dev |  145 MB |      true |      false |      6847 |       6847 |               6847 |        |
+----------------+----+------------+---------+-----------+------------+-----------+------------+--------------------+--------+

# List main.go keys.
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --keys-only --prefix / | grep main.go
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
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go | tail -n 20

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
$ docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go -w fields | grep -e Key -e Revision -e Version | grep -v Value
"Revision" : 6847
"Key" : "/etcdctl/main.go"
"CreateRevision" : 818
"ModRevision" : 6426
"Version" : 36

# Check the difference between the current and previous versions of the value for /etcdctl/main.go key.
$ diff <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get /etcdctl/main.go ) <( docker run --rm --entrypoint etcdctl --network=container:gitcd bitnami/etcd --insecure-transport get --rev=6425 /etcdctl/main.go )
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
$ docker volume rm gitcd-backend-repo
gitcd-backend-repo
```
