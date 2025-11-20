#!/bin/bash
# Download OpenJDK 21
JDK_URL="https://download.java.net/java/GA/jdk21.0.2/f2283984656d49d69e91c558476027ac/13/GPL/openjdk-21.0.2_macos-aarch64_bin.tar.gz"
OUTPUT_FILE="openjdk-21.tar.gz"
EXTRACT_DIR="jdk_local"

echo "Downloading OpenJDK 21..."
curl -L -o $OUTPUT_FILE $JDK_URL

echo "Extracting..."
mkdir -p $EXTRACT_DIR
tar -xzf $OUTPUT_FILE -C $EXTRACT_DIR

echo "Java setup complete."
