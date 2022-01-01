IMAGE_TAG = trishanku/gitcd:latest
BINDIR = ./bin
BACKEND_VERSION = `cat VERSION`

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

build:
	go build \
		-ldflags "-X github.com/trishanku/gitcd/pkg/backend.Version=${BACKEND_VERSION}" \
		-o "${BINDIR}/gitcd" \
		main.go

temp-cert: ensure-bin-dir
	openssl req -newkey rsa:4096 -x509 -sha256 -days 1 -nodes -extensions v3_req \
		-config hack/temp-cert.conf \
		-out "${BINDIR}/server.crt" -keyout "${BINDIR}/server.key" \

docker-build: check
	docker build -t "${IMAGE_TAG}" .
