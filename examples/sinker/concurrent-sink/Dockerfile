##
## Build context must be the repository root:
##   docker build -t concurrent-sink -f examples/sinker/concurrent-sink/Dockerfile .
##
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:sinker:concurrent-sink:installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /project/examples/sinker/concurrent-sink/build/install/concurrent-sink/ /app/
ENTRYPOINT ["/app/bin/concurrent-sink"]
