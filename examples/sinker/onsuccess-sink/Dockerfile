FROM amazoncorretto:11 AS build
WORKDIR /app
COPY . .
RUN ./gradlew :examples:sinker:onsuccess-sink:jar --no-daemon

FROM amazoncorretto:11
COPY --from=build /app/examples/sinker/onsuccess-sink/build/libs/*.jar /app/onsuccess-sink.jar
ENTRYPOINT ["java", "-jar", "/app/onsuccess-sink.jar"]
