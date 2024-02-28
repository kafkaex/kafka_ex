#!/bin/bash

# Setup directories
SSL_DIR="../ssl"
mkdir -p $SSL_DIR

# Password for the keystores and private keys
PASSWORD="kafka_ex"

# Generate the CA
openssl req -new -x509 -keyout "$SSL_DIR/ca-key" -out "$SSL_DIR/ca-cert" -days 365 \
-subj "/CN=localhost/OU=KafkaEx/O=KafkaEx/L=KafkaEx/ST=KafkaEx/C=US" -passout pass:$PASSWORD || exit 1

# Generate the server key
openssl genrsa -out "$SSL_DIR/server.key" 2048 || exit 1

# Generate the server certificate request
openssl req -new -key "$SSL_DIR/server.key" -out "$SSL_DIR/server.csr" \
-subj "/CN=localhost/OU=KafkaEx/O=KafkaEx/L=KafkaEx/ST=KafkaEx/C=US" -passout pass:$PASSWORD || exit 1

# Sign the server certificate with the CA
openssl x509 -req -in "$SSL_DIR/server.csr" -CA "$SSL_DIR/ca-cert" -CAkey "$SSL_DIR/ca-key" -CAcreateserial \
-out "$SSL_DIR/server.crt" -days 365 -passin pass:$PASSWORD || exit 1

# Convert the server certificate and key into a PKCS#12 file
openssl pkcs12 -export -in "$SSL_DIR/server.crt" -inkey "$SSL_DIR/server.key" -out "$SSL_DIR/server.p12" \
-name kafka -passin pass:$PASSWORD -passout pass:$PASSWORD || exit 1

# Create Kafka server keystore
keytool -importkeystore -destkeystore "$SSL_DIR/kafka.server.keystore.jks" -srckeystore "$SSL_DIR/server.p12" \
-srcstoretype PKCS12 -alias kafka -storepass $PASSWORD -srcstorepass $PASSWORD || exit 1

# Create Kafka server truststore and import the CA certificate
keytool -keystore "$SSL_DIR/kafka.server.truststore.jks" -alias CARoot -import -file "$SSL_DIR/ca-cert" \
-storepass $PASSWORD -noprompt || exit 1

# Prepare PEM files for client
cp "$SSL_DIR/server.key" "$SSL_DIR/key.pem" || exit 1
cp "$SSL_DIR/server.crt" "$SSL_DIR/cert.pem" || exit 1
cp "$SSL_DIR/ca-cert" "$SSL_DIR/ca-cert" || exit 1

echo "SSL files generated successfully."
