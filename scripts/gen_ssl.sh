#!/bin/bash
set -euo pipefail

# Setup directories
SSL_DIR="../ssl"
mkdir -p "$SSL_DIR"

# Password for the keystores and private keys
PASSWORD="kafka_ex"

# Generate the CA
openssl req -new -x509 -keyout "$SSL_DIR/ca-key" -out "$SSL_DIR/ca-cert" -days 365 \
  -subj "/CN=localhost/OU=KafkaEx/O=KafkaEx/L=KafkaEx/ST=KafkaEx/C=US" -passout pass:$PASSWORD

# Generate the server key
openssl genrsa -out "$SSL_DIR/server.key" 2048

# Generate the server certificate request
openssl req -new -key "$SSL_DIR/server.key" -out "$SSL_DIR/server.csr" \
  -subj "/CN=localhost/OU=KafkaEx/O=KafkaEx/L=KafkaEx/ST=KafkaEx/C=US"

# Sign the server certificate with the CA
openssl x509 -req -in "$SSL_DIR/server.csr" -CA "$SSL_DIR/ca-cert" -CAkey "$SSL_DIR/ca-key" -CAcreateserial \
  -out "$SSL_DIR/server.crt" -days 365 -passin pass:$PASSWORD

# ---- PKCS#12 outputs

# Keystore (PKCS#12) with full chain
openssl pkcs12 -export \
  -in "$SSL_DIR/server.crt" \
  -inkey "$SSL_DIR/server.key" \
  -certfile "$SSL_DIR/ca-cert" \
  -name kafka \
  -out "$SSL_DIR/kafka.server.keystore.p12" \
  -passout pass:$PASSWORD \
  -keypbe PBE-SHA1-3DES \
  -certpbe PBE-SHA1-3DES \
  -macalg sha1 \
  -legacy

# Truststore (PKCS#12) containing CA
openssl pkcs12 -export \
  -in "$SSL_DIR/ca-cert" \
  -nokeys \
  -name CARoot \
  -out "$SSL_DIR/kafka.server.truststore.p12" \
  -passout pass:$PASSWORD \
  -certpbe PBE-SHA1-3DES \
  -macalg sha1 \
  -legacy

# Optional: PEMs for clients
cp "$SSL_DIR/server.key" "$SSL_DIR/key.pem"
cp "$SSL_DIR/server.crt" "$SSL_DIR/cert.pem"

echo "SSL files (PKCS#12) generated successfully:"
ls -l "$SSL_DIR"/kafka.server.keystore.p12 "$SSL_DIR"/kafka.server.truststore.p12
