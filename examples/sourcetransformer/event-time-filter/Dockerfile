##
## Build context must be the repository root:
##   docker build -t event-time-filter -f examples/sourcetransformer/event-time-filter/Dockerfile .
##
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:sourcetransformer:event-time-filter:installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /project/examples/sourcetransformer/event-time-filter/build/install/event-time-filter/ /app/
ENTRYPOINT ["/app/bin/event-time-filter"]
