package com.epam.dataflow;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.dbcp2.PoolingDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

public class ReadFromJdbcFn extends PTransform<PBegin, PCollection<String>> {

    private JdbcToCsvOptions options;

    public ReadFromJdbcFn(JdbcToCsvOptions options) {
        this.options = options;
    }

    @Override
    public PCollection<String> expand(PBegin input) {

        final DataSource dataSource = PoolProvider.of(options.getDriverName(), options.getConnectionString(),
                options.getUser(),
                options.getPassword(), options.getMinPoolSize(), options.getMaxPoolSize()).getDataSource();

        return input.apply(JdbcIO.<String>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                .withCoder(StringUtf8Coder.of())
                .withFetchSize(options.getFetchSize())
                .withQuery(options.getQuery())
                .withRowMapper(new JdbcToCsvRowMapper()));

        /*final JdbcIO.DataSourceConfiguration dataSourceConfiguration = configure(options.getConnectionString(),
                options.getDriverName(), options.getUser(),
                options.getPassword(), options.getConnectionProperties());

        final SerializableFunction<Void, DataSource> dataSourceProviderFunction =
                JdbcIO.PoolableDataSourceProvider.of(dataSourceConfiguration);

        return input.apply(
                JdbcIO.<String>read()
                        .withDataSourceProviderFn(dataSourceProviderFunction)
                        .withCoder(StringUtf8Coder.of())
                        .withFetchSize(options.getFetchSize())
                        .withQuery(options.getQuery())
                        .withRowMapper(new JdbcToCsvRowMapper()));*/
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
