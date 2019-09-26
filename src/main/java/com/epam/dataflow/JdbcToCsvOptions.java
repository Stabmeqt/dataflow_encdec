package com.epam.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;

public interface JdbcToCsvOptions extends DataflowPipelineOptions {

    String getConnectionString();
    void setConnectionString(String connectionString);

    String getConnectionProperties();
    void setConnectionProperties(String connectionProperties);

    String getDriverName();
    void setDriverName(String driverName);

    String getQuery();
    void setQuery(String query);

    String getOutputBucket();
    void setOutputBucket(String outputBucket);

    String getUser();
    void setUser(String user);

    String getPassword();
    void setPassword(String password);

    @Default.Integer(50_000)
    int getFetchSize();
    void setFetchSize(int fetchSize);
}
