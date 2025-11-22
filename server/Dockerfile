# Base image
FROM ubuntu:22.04

# Set working directory
WORKDIR /app

# Install build tools and dependencies
RUN apt-get update && \
    apt-get install -y build-essential g++ libpq-dev libcurl4-openssl-dev nlohmann-json3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy server source code
COPY . /app

# Compile kv_server and CivetWeb from source
RUN g++ -std=c++17 \
          -I/usr/include/postgresql \
          -I./civetweb/include \
          -DNO_SSL \
          main.cpp \
          civetweb/src/civetweb.c \
          civetweb/src/*.cpp \
          -L/usr/lib/x86_64-linux-gnu -lpq -lcurl -pthread \
          -fpermissive \
          -o kv_server

# Expose server port
EXPOSE 8080

# Default command
CMD ["./kv_server"]
