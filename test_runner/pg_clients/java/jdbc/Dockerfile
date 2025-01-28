FROM openjdk:22@sha256:b7d44427f4622d3f6b9a60583e5218ecfa8b4e44f3e01dfd0d9b7d7abba31c9a
WORKDIR /source

COPY . .

WORKDIR /app
RUN curl --output postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.2.jar && \
    javac -d /app /source/Example.java

CMD ["java", "-cp", "/app/postgresql.jar:.", "Example"]
