##
## Build context must be the repository root:
##   docker build -t simple-map -f examples/mapper/simple-map/Dockerfile .
##
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:mapper:simple-map:installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /project/examples/mapper/simple-map/build/install/simple-map/ /app/
ENTRYPOINT ["/app/bin/simple-map"]
