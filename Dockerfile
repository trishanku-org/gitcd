FROM golang:1.17-alpine3.14 AS builder

RUN apk update && apk add libc-dev gcc pkgconfig libgit2-dev

WORKDIR /trishanku/gitcd

COPY cmd cmd/
COPY pkg pkg/
COPY tools tools/
COPY vendor vendor/
COPY go.mod go.sum main.go VERSION ./

RUN go build -mod=vendor \
		-o /gitcd \
		-ldflags "-X github.com/trishanku/gitcd/pkg/backend.Version=$(cat VERSION)" \
		main.go

RUN cat /gitcd

FROM alpine:3.14 AS runner

RUN apk update && apk add libgit2

COPY --from=builder /gitcd /gitcd

ENTRYPOINT [ "/gitcd" ]