package com.epam.dataflow.transform;

import com.epam.dataflow.*;
import com.epam.dataflow.jdbc.JdbcToCsvRowMapper;
import com.epam.dataflow.jdbc.PoolProvider;
import com.epam.dataflow.jdbc.RangeHolder;
import com.epam.dataflow.util.Util;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import javax.sql.DataSource;

public class ReadFromJdbcFn extends PTransform<PBegin, PCollection<String>> {

    private int fetchSize;
    private DataSource dataSource;
    private String rowCountQuery;
    private String selectQuery;

    public ReadFromJdbcFn(JdbcToCsvOptions options) {
        fetchSize = options.getFetchSize();
        dataSource = PoolProvider.of(options.getDriverName(), options.getConnectionString(),
                options.getUser(),
                options.getPassword(), options.getMinPoolSize(), options.getMaxPoolSize()).getDataSource();
        rowCountQuery = String.format("SELECT count(%s) FROM %s",
                options.getPagingColumn(), Util.getTableNameFromQuery(options.getQuery().trim()));
        selectQuery =
                String.format("%s WHERE %s >= ? and %s < ?", options.getQuery().trim(),
                        options.getPagingColumn(), options.getPagingColumn());
    }

    @Override
    public PCollection<String> expand(PBegin input) {

        final JdbcIO.DataSourceConfiguration dataSourceConfiguration = JdbcIO.DataSourceConfiguration.create(dataSource);

        final PCollection<RangeHolder> rangeHolders = input.apply("Read record count",
                JdbcIO.<Long>read()
                        .withDataSourceConfiguration(dataSourceConfiguration)
                        .withCoder(AvroCoder.of(Long.class))
                        .withQuery(rowCountQuery)
                        .withRowMapper(resultSet -> resultSet.getLong(1)))
                .apply("Distribute ranges",
                        ParDo.of(new DoFn<Long, RangeHolder>() {
                            @ProcessElement
                            public void processElement(@Element Long count, OutputReceiver<RangeHolder> out) {
                                int rangeCount = (int) (count / fetchSize);
                                for (int i = 0; i < rangeCount; i++) {
                                    out.output(new RangeHolder(i * fetchSize, (i + 1) * fetchSize));
                                }
                                if (count > rangeCount * fetchSize) {
                                    out.output(new RangeHolder(rangeCount * fetchSize,
                                            rangeCount * fetchSize + count % fetchSize));
                                }
                            }
                        }))
                .apply("Repartition", Repartition.of());

        return rangeHolders
                .apply(JdbcIO.<RangeHolder, String>readAll()
                        .withOutputParallelization(true)
                        .withDataSourceConfiguration(dataSourceConfiguration)
                        .withParameterSetter((element, preparedStatement) -> {
                            preparedStatement.setLong(1, element.getRangeStart());
                            preparedStatement.setLong(2, element.getRangeEnd());
                        })
                        .withCoder(StringUtf8Coder.of())
                        .withFetchSize(fetchSize)
                        .withQuery(selectQuery)
                        .withRowMapper(new JdbcToCsvRowMapper()));
    }
}
