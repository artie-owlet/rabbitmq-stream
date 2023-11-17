#!/bin/bash

set -e

rm -f ca.crt ca.key client.pfx server.crt server.key
rm -rf ./rabbitmq-ssl
mkdir ./rabbitmq-ssl

openssl req -newkey rsa:4096 -x509 -days 365 -keyout ca.key -out ca.crt -subj '/CN=unknown' -noenc &> /dev/null

openssl req -newkey rsa:4096 -keyout server.key -out server.csr -subj '/CN=localhost' -noenc &> /dev/null
openssl x509 -CAcreateserial -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key -out server.crt &> /dev/null
rm server.csr

openssl req -newkey rsa:4096 -keyout client.key -out client.csr -subj '/CN=guest' -noenc &> /dev/null
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key -out client.crt &> /dev/null
rm client.csr

cat client.key client.crt ca.crt > client.pem
openssl pkcs12 -export -out client.pfx -inkey client.key -in client.pem -certfile ca.crt -passout pass:
rm client.key client.crt client.pem

mkdir -p rabbitmq-ssl
mv ca.crt ca.key server.crt server.key ./rabbitmq-ssl/
