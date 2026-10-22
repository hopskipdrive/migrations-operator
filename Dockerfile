# Build the manager binary
FROM golang:1.24 AS builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY main.go main.go
COPY api/ api/
COPY cmd/ cmd/
COPY components/ components/
COPY controllers/ controllers/
COPY utils/ utils/
COPY webhook/ webhook/
COPY http/ http/
COPY stubs/ stubs/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GO111MODULE=on go build -a -tags release -o manager main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GO111MODULE=on go build -a -tags release -o waiter cmd/waiter/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot AS runtime-base

FROM 564365460806.dkr.ecr.us-east-2.amazonaws.com/docker-hub/library/python:3.13.7-slim AS certificates
COPY --from=runtime-base /etc/ssl/certs/ca-certificates.crt /tmp/original.crt
COPY filter_ca_bundle.py /tmp/filter_ca_bundle.py
RUN python3 /tmp/filter_ca_bundle.py /tmp/original.crt /tmp/ca-certificates.crt

FROM runtime-base
COPY --from=certificates /tmp/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
WORKDIR /
COPY --from=builder /workspace/manager /workspace/waiter ./
USER nonroot:nonroot

ENTRYPOINT ["/manager"]
