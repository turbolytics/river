# rivercheck, built against River from this checkout.
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN cd temper/recipe/go && GOWORK=off CGO_ENABLED=0 go build -o /rivercheck .

FROM alpine:3.20
COPY --from=build /rivercheck /usr/local/bin/rivercheck
