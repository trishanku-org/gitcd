IMAGE_TAG ?= trishanku/gitcd:latest
BINDIR ?= ./bin
BACKEND_VERSION = `cat VERSION`
DOCKER_RUN_OPTS ?= -d --rm --tmpfs /tmp/trishanku/gitcd:rw,noexec,nosuid,size=65536k --name gitcd

ensure-bin-dir:
	mkdir -p "${BINDIR}"

cleanup:
	rm -rf "${BINDIR}"

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
		-o "${BINDIR}/gitcd" \
		main.go

run: check
	go run \
		-ldflags "-X github.com/trishanku/gitcd/pkg/backend.Version=${BACKEND_VERSION}" \
		main.go ${RUN_ARGS}

temp-cert: ensure-bin-dir
	openssl req -newkey rsa:4096 -x509 -sha256 -days 1 -nodes -extensions v3_req \
		-config hack/temp-cert.conf \
		-out "${BINDIR}/tls.crt" -keyout "${BINDIR}/tls.key" \

docker-build:
	docker build -t "${IMAGE_TAG}" .

docker-run:
	docker run ${DOCKER_RUN_OPTS} "${IMAGE_TAG}" ${RUN_ARGS}

start-docker-gitcd-kube:
	hack/kube/start.sh

stop-docker-gitcd-kube:
	hack/kube/stop.sh

cleanup-docker-gitcd-kube:
	hack/kube/cleanup.sh

start-docker-registry: docker-build
	hack/registry/start.sh

stop-docker-registry: 
	hack/registry/stop.sh

start-k8s-certmanager:
	hack/kubernetes/certmanager/start.sh

stop-k8s-certmanager:
	hack/kubernetes/certmanager/stop.sh
