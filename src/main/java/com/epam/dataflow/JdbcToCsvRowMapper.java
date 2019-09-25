package com.epam.dataflow;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;

public class JdbcToCsvRowMapper implements JdbcIO.RowMapper<String> {

    private boolean isEncrypt;
    private String projectId;
    private String location;
    private String keyring;
    private String key;

    public JdbcToCsvRowMapper(JdbcToCsvOptions options) {
        isEncrypt = options.isEncrypt();
        projectId = options.getProject();
        location = options.getLocation();
        keyring = options.getKeyring();
        key = options.getKey();
    }

    @Override
    public String mapRow(ResultSet resultSet) throws Exception {
        final StringWriter stringWriter = new StringWriter();
        resultSet.beforeFirst();
        try {
            CSVPrinter csvPrinter =
                    new CSVPrinter(
                            stringWriter,
                            CSVFormat.DEFAULT
                                    .withHeader(resultSet)
                                    .withQuoteMode(QuoteMode.NON_NUMERIC)
                    );
            csvPrinter.printRecords(resultSet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (isEncrypt) {
            final byte[] plainTextBytes = stringWriter.toString().getBytes();
            return new String(Util.encrypt(
                    projectId,
                    location,
                    keyring,
                    key,
                    plainTextBytes));
        }

        return stringWriter.toString();
    }
}
