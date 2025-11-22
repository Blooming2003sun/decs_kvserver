# Base image
FROM ubuntu:22.04

# Set working directory
WORKDIR /app

# Install build tools and dependencies
RUN apt-get update && \
    apt-get install -y build-essential g++ libpq-dev libcurl4-openssl-dev nlohmann-json3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy loadtester source code
COPY . /app

# Compile loadtester with CivetWeb and dependencies
RUN g++ -std=c++17  -g \ 
          -I/usr/include/postgresql \
          -I./civetweb/include \
          -DNO_SSL \
          random_request.cpp \
          civetweb/src/civetweb.c \
          civetweb/src/*.cpp \
          -L/usr/lib/x86_64-linux-gnu -lpq -lcurl -pthread \
          -fpermissive \
          -o loadtester

# Default command
CMD ["./loadtester"]
