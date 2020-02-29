FROM golang:1.14-alpine

WORKDIR /go/src/app

RUN go mod download && go mod tidy