##
## Build context must be the repository root:
##   docker build -t onsuccess-sink -f examples/sinker/onsuccess-sink/Dockerfile .
##
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:sinker:onsuccess-sink:installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /project/examples/sinker/onsuccess-sink/build/install/onsuccess-sink/ /app/
ENTRYPOINT ["/app/bin/onsuccess-sink"]
