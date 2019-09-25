# dataflow_encdec

Prepare a Cloud SQL instance and fill it with data.
Create a keyring and a symmetric key for encryption/decryption

Build with maven
```bash
mvn clean package
```
Execute the jar with the following command (make sure to change relevant properties from example to real ones):
```bash
java -jar jdbcToCsv-1.0-SNAPSHOT.jar \
--cloudSqlInstance=<projectId>:<zoneId>:<instanceId> \
--database=<databaseName> \
--user=<dbUser> \
--password=<dbUserPassword> \
--location=<keyring location> \
--keyring=<keyring name> \
--key=<key name> \
--outputBucket=<bucket name> \
--runner=DataflowRunner \
--gcpTempLocation=gs://<bucket name>/temporary  \
--project=<projectId> \
--query="SELECT * FROM <table name> LIMIT 10"
```