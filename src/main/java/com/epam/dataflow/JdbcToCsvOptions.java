package com.epam.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;

public interface JdbcToCsvOptions extends DataflowPipelineOptions {

    String getLocation();
    void setLocation(String location);

    String getKeyring();
    void setKeyring(String keyring);

    String getKey();
    void setKey(String key);

    String getKeyVersion();
    void setKeyVersion(String keyVersion);

    String getQuery();
    void setQuery(String query);

    String getOutputBucket();
    void setOutputBucket(String outputBucket);

    String getKeyName();
    void setKeyName(String keyName);

    @Default.Boolean(false)
    Boolean isEncrypt();
    void setEncrypt(Boolean encrypt);

    @Default.Boolean(false)
    Boolean isDecrypt();
    void setDecrypt(Boolean decrypt);

    String getCloudSqlInstance();
    void setCloudSqlInstance(String cloudSqlInstance);

    String getDatabase();
    void setDatabase(String database);

    String getUser();
    void setUser(String user);

    String getPassword();
    void setPassword(String password);
}
