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
[main (root-commit) 78306e3] init
 1 file changed, 0 insertions(+), 0 deletions(-)
 create mode 100644 init
+ docker run --rm -v gitcd-backend:/backend trishanku/gitcd:latest init --repo=/backend
{"level":"info","ts":1643036214.5228558,"logger":"init.refs/heads/main","caller":"cmd/init.go:84","msg":"Initializing","repoPath":"/backend","options":{"Repo":{},"Errors":{},"DataRefName":"refs/heads/main","MetadataRefNamePrefix":"refs/gitcd/metadata/","StartRevision":1,"Version":"v0.0.1-dev","Force":false,"CommitterName":"trishanku","CommitterEmail":"trishanku@heaven.com"}}
{"level":"info","ts":1643036214.5409706,"logger":"init.refs/heads/main","caller":"cmd/init.go:91","msg":"Initialized successfully"}
+ docker run --name gitcd -d -v gitcd-backend:/backend trishanku/gitcd:latest serve --repo=/backend --debug
f1f71deb131321cf4f7e7070f1a7cf4da2f35bec8caadc0eded6948e16d03a3e
+ docker run --rm --network=container:gitcd --entrypoint etcdctl bitnami/etcd:3 --insecure-transport endpoint status
127.0.0.1:2379, 0, v0.0.1-dev, 161 kB, true, false, 1, 1, 1, 
+ prepare_certs
+ docker volume create kube-certs
kube-certs
+ docker run -i --rm -v kube-certs:/tls -w /tls busybox:1 dd of=tls.conf
0+1 records in
0+1 records out
482 bytes (482B) copied, 0.001967 seconds, 239.3KB/s
+ docker run --rm -v kube-certs:/tls -w /tls --entrypoint openssl nginx:1 req -newkey rsa:4096 -x509 -sha256 -days 365 -nodes -extensions v3_req -config tls.conf -out tls.crt -keyout tls.key
Generating a RSA private key
........................++++
.........................................................++++
writing new private key to 'tls.key'
-----
+ docker run --rm -v kube-certs:/tls -w /tls busybox:1 ln -s tls.crt ca.crt
+ start_kube_apiserver
+ docker run --name kube-apiserver -d -v kube-certs:/tls --network=container:gitcd --entrypoint kube-apiserver k8s.gcr.io/kube-apiserver:v1.23.2 --client-ca-file=/tls/ca.crt --etcd-compaction-interval=0 --etcd-servers=http://localhost:2379 --secure-port=6443 --service-account-issuer=https://kube-apiserver/ --service-account-key-file=/tls/tls.key --service-account-signing-key-file=/tls/tls.key --storage-media-type=application/yaml --tls-cert-file=/tls/tls.crt --tls-private-key-file=/tls/tls.key --watch-cache=false
77e7851c702efeb714c422b83ff361334d33e73a2a2a9abceba5680c199cd8f0

# Check that kube-apiserver and Gitcd containers are running.
$ docker ps -n 2
CONTAINER ID   IMAGE                               COMMAND                  CREATED         STATUS                  PORTS     NAMES
77e7851c702e   k8s.gcr.io/kube-apiserver:v1.23.2   "kube-apiserver --cl…"   1 second ago    Up Less than a second             kube-apiserver
f1f71deb1313   trishanku/gitcd:latest              "/gitcd serve --repo…"   5 seconds ago   Up 3 seconds                      gitcd

# Wait for kube-apiserver to initialize.
$ sleep 10

# Inspect the Kubernetes content in the backend Git repo.
$ echo "git reset --hard && git checkout refs/gitcd/metadata/refs/heads/main && git checkout main" | \
    docker run -i --rm -v gitcd-backend:/backend -w /backend bitnami/git:2 sh
HEAD is now at 2056d4d 61
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

HEAD is now at d63fe01 61
Previous HEAD position was d63fe01 61
Switched to branch 'main'

$ echo "apk update && apk add tree && tree /backend" | docker run -i --rm -v gitcd-backend:/backend alpine:3 sh
fetch https://dl-cdn.alpinelinux.org/alpine/v3.15/main/x86_64/APKINDEX.tar.gz
fetch https://dl-cdn.alpinelinux.org/alpine/v3.15/community/x86_64/APKINDEX.tar.gz
v3.15.0-226-g0aa9a217b1 [https://dl-cdn.alpinelinux.org/alpine/v3.15/main]
v3.15.0-227-ga87054a505 [https://dl-cdn.alpinelinux.org/alpine/v3.15/community]
OK: 15848 distinct packages available
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
    ├── flowschemas
    │   ├── catch-all
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
    ├── namespaces
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
    └── ranges
        ├── serviceips
        └── servicenodeports

10 directories, 58 files

$ docker run --rm -v gitcd-backend:/backend busybox:1 cat /backend/registry/namespaces/kube-system
apiVersion: v1
kind: Namespace
metadata:
  creationTimestamp: "2022-01-24T14:57:01Z"
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
    manager: Go-http-client
    operation: Update
    time: "2022-01-24T14:57:01Z"
  name: kube-system
  uid: c60ad5a2-9612-4389-bb85-6b26b6d0804c
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
