##
## Build context must be the repository root:
##   docker build -t concurrent-sink -f examples/sinker/concurrent-sink/Dockerfile .
##
FROM amazoncorretto:11 AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:sinker:concurrent-sink:jar --no-daemon

FROM amazoncorretto:11
COPY --from=builder /project/examples/sinker/concurrent-sink/build/libs/concurrent-sink.jar /app/concurrent-sink.jar
ENTRYPOINT ["java", "-jar", "/app/concurrent-sink.jar"]
