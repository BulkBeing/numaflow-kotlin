##
## Build context must be the repository root:
##   docker build -t word-splitter -f examples/mapstreamer/word-splitter/Dockerfile .
##
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:mapstreamer:word-splitter:installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /project/examples/mapstreamer/word-splitter/build/install/word-splitter/ /app/
ENTRYPOINT ["/app/bin/word-splitter"]
