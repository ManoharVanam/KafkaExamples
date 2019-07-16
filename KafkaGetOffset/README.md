# KafkaClients
### Build
```
mvn clean package
copy the jar from target/kafka-clients-offset-1.0-SNAPSHOT.jar to KAFKA_HOME/libs dir
execute CustomGetOffsetShell command:
./kafka-run-class.sh kafka.tools.CustomGetOffsetShell  --broker-list c318-node2.squadron-labs.com:6668 --topic test123 --consumer.config ssl.properties
cat ssl.properties
security.protocol=SASL_SSL
ssl.truststore.location=/opt/ssl/kafka.client.truststore.jks
ssl.truststore.password=12345678
ssl.keystore.location=/opt/ssl/kafka.client.keystore.jks
ssl.keystore.password=12345678
ssl.key.password=12345678
```
