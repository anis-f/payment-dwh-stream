FROM java
ADD target/payment-dwh-stream-service-0.0.1-SNAPSHOT.jar //
ENTRYPOINT ["java", "-jar", "/payment-dwh-stream-service-0.0.1-SNAPSHOT.jar"]
