##
## Build context must be the repository root:
##   docker build -t flatmap -f examples/mapper/flatmap/Dockerfile .
##
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /project
COPY . .
RUN ./gradlew :examples:mapper:flatmap:installDist --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /project/examples/mapper/flatmap/build/install/flatmap/ /app/
ENTRYPOINT ["/app/bin/flatmap"]
