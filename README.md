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
--driverName=<driver name> \
--minPoolSize=<minimal (and initial) pool size> \
--maxPoolSize=<maximal pool size> \
--connectionString="<full DB connection string>" \
--user=<dbUser> \
--password=<dbUserPassword> \
--outputBucket=<bucket name> \
--runner=DataflowRunner \
--gcpTempLocation=gs://<bucket name>/temporary  \
--project=<projectId> \
--query="<your query text here>" \
--fetchSize=<fetch size>
```

Fetch size is the size of the data that is going to be fetched and loaded in memory per every database call. It is
 optional and defaults to 50000. Currently it is not used, so setting it will have no impact at all.

MinPoolSize - this setting defaults to 1 if omitted.

MaxPoolSize - this setting defaults to 10 if omitted.