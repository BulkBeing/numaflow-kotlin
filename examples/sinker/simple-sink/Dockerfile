##
## Build context must be the repository root:
##   docker build -t simple-sink -f examples/sinker/simple-sink/Dockerfile .
##
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:sinker:simple-sink:installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /project/examples/sinker/simple-sink/build/install/simple-sink/ /app/
ENTRYPOINT ["/app/bin/simple-sink"]
