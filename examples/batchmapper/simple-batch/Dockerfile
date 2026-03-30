##
## Build context must be the repository root:
##   docker build -t simple-batch -f examples/batchmapper/simple-batch/Dockerfile .
##
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:batchmapper:simple-batch:installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /project/examples/batchmapper/simple-batch/build/install/simple-batch/ /app/
ENTRYPOINT ["/app/bin/simple-batch"]
