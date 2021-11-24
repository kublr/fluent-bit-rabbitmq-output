FROM golang:1.17  as building-stage

COPY ./*.go /go/src/
COPY ./go.mod /go/src/
COPY ./go.sum /go/src/

COPY ./Makefile /go/src

WORKDIR /go/src/

RUN go mod download && make

FROM fluent/fluent-bit:1.8.10

LABEL maintainer="Björn Franke"

COPY --from=building-stage /go/src/out_rabbitmq.so  /fluent-bit/bin/
COPY ./conf/fluent-bit-docker.conf /fluent-bit/etc

EXPOSE 2020

CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit-docker.conf","-e","/fluent-bit/bin/out_rabbitmq.so"]
