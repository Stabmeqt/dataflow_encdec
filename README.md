# dataflow_encdec

Prepare a Cloud SQL instance and fill it with data.
Create a keyring and a symmetric key for encryption/decryption.

Pipeline will read data from JDBC datasource, encrypt it with provided key and output it to Google Cloud Storage.
To check encryption correctness, run the pipeline with ```--decrypt``` flag.

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
--fetchSize=<fetch size> \
--pagingColumn=<row number column> \
--keyPath=gs://<bucket name>/<path to key> \
--decrypt
```

Fetch size is the size of the data that is going to be fetched and loaded in memory per every database call. It is used
with paging column to construct query pagination.

MinPoolSize - this setting defaults to 1 if omitted.

MaxPoolSize - this setting defaults to 10 if omitted.