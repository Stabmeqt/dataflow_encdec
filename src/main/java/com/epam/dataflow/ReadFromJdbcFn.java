package com.epam.dataflow;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class ReadFromJdbcFn extends PTransform<PBegin, PCollection<String>> {

    private JdbcToCsvOptions options;

    public ReadFromJdbcFn(JdbcToCsvOptions options) {
        this.options = options;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
        return input.apply(
                JdbcIO.<String>read()
                        .withDataSourceConfiguration(
                                configure(options.getConnectionString(), options.getDriverName(), options.getUser(),
                                        options.getPassword(), options.getConnectionProperties()))
                        .withCoder(StringUtf8Coder.of())
                        .withFetchSize(options.getFetchSize())
                        .withQuery(options.getQuery())
                        .withRowMapper(new JdbcToCsvRowMapper()));
    }

    private static JdbcIO.DataSourceConfiguration configure(String connString, String driver, String user, String password,
                                                            String connProperties) {

        final JdbcIO.DataSourceConfiguration dataSourceConfiguration = JdbcIO.DataSourceConfiguration.create(driver, connString)
                .withUsername(user)
                .withPassword(password);

        return connProperties == null ?
                dataSourceConfiguration : dataSourceConfiguration.withConnectionProperties(connProperties);
    }

}
