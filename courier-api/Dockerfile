# Use JRE not JDK - only need to run, not compile
# Alpine base keeps the image small (~100MB vs ~300MB for standard)
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Copy the fat JAR built by spring-boot-maven-plugin
COPY target/courier-api-1.0.0.jar app.jar

# Document which port this service listens on
EXPOSE 8080

# Use exec form - signals go directly to the JVM, not a shell
ENTRYPOINT ["java", "-jar", "app.jar"]