install-requirements:
	go install -mod vendor \
		github.com/golang/mock/mockgen \
		github.com/onsi/ginkgo/ginkgo

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
	ACK_GINKGO_RC=true ginkgo ./pkg/...
