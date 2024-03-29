IMAGE_TAG ?= asia-south1-docker.pkg.dev/trishanku/trishanku/gitcd:latest
BACKEND_VERSION = `cat VERSION`
DOCKER_RUN_OPTS ?= -d --rm --tmpfs /tmp/trishanku/gitcd:rw,noexec,nosuid,size=65536k --name gitcd

ensure-bin-dir:
	mkdir -p ./bin

cleanup:
	rm -rf ./bin

install-requirements:
	go install -mod vendor \
		github.com/golang/mock/mockgen \
		github.com/onsi/ginkgo/ginkgo \
		github.com/spf13/cobra/cobra

revendor:
	go mod vendor -v
	go mod tidy -v

update-dependencies:
	go get -u

meta-generate:
	hack/meta-generate.sh

generate: meta-generate
	go generate ./pkg/...

check:
	go fmt ./pkg/... 
	go vet ./pkg/...

test: check
	ACK_GINKGO_RC=true ginkgo -mod=vendor ${GINKGO_OPTS} ./pkg/...

build: check
	go build \
		-ldflags "-X github.com/trishanku/gitcd/pkg/backend.Version=${BACKEND_VERSION}" \
		-o "./bin/gitcd" \
		main.go

run: check
	go run \
		-ldflags "-X github.com/trishanku/gitcd/pkg/backend.Version=${BACKEND_VERSION}" \
		main.go ${RUN_ARGS}

start-docker-gitcd-kube-apiserver:
	hack/kube/start.sh

stop-docker-gitcd-kube-apiserver:
	hack/kube/stop.sh

cleanup-docker-gitcd-kube-apiserver:
	hack/kube/cleanup.sh

build-kubernetes-secrets-volume: 
	./hack/kubernetes/the-hard-way/secrets/generate.sh

cleanup-kubernetes-secrets-volume:
	./hack/kubernetes/the-hard-way/secrets/cleanup.sh

start-kubernetes-docker-control-plane:
	./hack/kubernetes/the-hard-way/control-plane/start.sh

stop-kubernetes-docker-control-plane:
	./hack/kubernetes/the-hard-way/control-plane/stop.sh

cleanup-kubernetes-docker-control-plane:
	./hack/kubernetes/the-hard-way/control-plane/cleanup.sh

add-trishanku-docker-repo-credentials:
	./hack/trishanku/the-hard-way/start.sh repo-credentials

start-trishanku-docker-control-plane:
	./hack/trishanku/the-hard-way/start.sh control-plane

start-trishanku-docker-worker:
	./hack/trishanku/the-hard-way/start.sh worker

stop-trishanku-docker:
	./hack/trishanku/the-hard-way/stop.sh

cleanup-trishanku-docker:
	./hack/trishanku/the-hard-way/cleanup.sh

temp-cert: ensure-bin-dir
	openssl req -newkey rsa:4096 -x509 -sha256 -days 1 -nodes -extensions v3_req \
		-config hack/temp-cert.conf \
		-out "./bin/tls.crt" -keyout "./bin/tls.key" \

docker-build:
	docker build -t "${IMAGE_TAG}" .

docker-run:
	docker run ${DOCKER_RUN_OPTS} "${IMAGE_TAG}" ${RUN_ARGS}

start-docker-gitcd-kind:
	hack/kubernetes/kind/start.sh

stop-docker-gitcd-kind:
	hack/kubernetes/kind/stop.sh

cleanup-docker-gitcd-kind:
	hack/kubernetes/kind/cleanup.sh

start-docker-registry: docker-build
	hack/registry/start.sh

stop-docker-registry: 
	hack/registry/stop.sh

start-k8s-certmanager:
	hack/kubernetes/kindrnetes/in-kubernetes/certmanager/start.sh

stop-k8s-certmanager:
	hack/kubernetes/kindrnetes/in-kubernetes/certmanager/stop.sh
