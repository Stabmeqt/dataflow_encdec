package com.epam.dataflow.jdbc;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;

public class JdbcToCsvRowMapper implements JdbcIO.RowMapper<String> {

    @Override
    public String mapRow(ResultSet resultSet) throws Exception {
        final StringWriter stringWriter = new StringWriter();
        resultSet.beforeFirst();
        try {
            CSVPrinter csvPrinter =
                    new CSVPrinter(
                            stringWriter,
                            CSVFormat.DEFAULT
                                    .withQuoteMode(QuoteMode.NON_NUMERIC)
                    );
            csvPrinter.printRecords(resultSet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return stringWriter.toString();
    }
}
