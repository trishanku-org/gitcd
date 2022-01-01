FROM golang:1.17-bullseye AS builder

RUN apt update && apt install -y pkg-config libgit2-dev

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

FROM debian:bullseye AS runner

RUN apt update && apt install -y libgit2-1.1

COPY --from=builder /gitcd /gitcd

CMD ["/gitcd"]