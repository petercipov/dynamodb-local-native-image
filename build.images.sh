#!/bin/bash

apt-get update
apt-get install -y curl

curl -fsSL -o dist.tar.gz  https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.3.0/graalvm-ce-java11-linux-amd64-21.3.0.tar.gz
tar -xzf dist.tar.gz
export PATH=/build/graalvm-ce-java11-21.3.0/bin:$PATH

gu install native-image

curl -o  apache-maven-3.8.4-bin.tar.gz https://dlcdn.apache.org/maven/maven-3/3.8.4/binaries/apache-maven-3.8.4-bin.tar.gz
tar -xzf ./apache-maven-3.8.4-bin.tar.gz
./apache-maven-3.8.4/bin/mvn clean package

cd server && sh ./build-image.sh && cd ..
