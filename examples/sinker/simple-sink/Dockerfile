##
## Build context must be the repository root:
##   docker build -t simple-sink -f examples/sinker/simple-sink/Dockerfile .
##
FROM amazoncorretto:11 AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:sinker:simple-sink:jar --no-daemon

FROM amazoncorretto:11
COPY --from=builder /project/examples/sinker/simple-sink/build/libs/simple-sink.jar /app/simple-sink.jar
ENTRYPOINT ["java", "-jar", "/app/simple-sink.jar"]
