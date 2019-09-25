package com.epam.dataflow;

import com.google.cloud.kms.v1.*;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {

        JdbcToCsvOptions pipelineOptions = null;
        try {
            pipelineOptions = PipelineOptionsFactory
                    .fromArgs(args)
                    .as(JdbcToCsvOptions.class);

        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            PipelineOptionsFactory.printHelp(System.err, JdbcToCsvOptions.class);
            System.exit(1);
        }

        final String connectionString =
                String.format("jdbc:mysql://google/%s?cloudSqlInstance=%s&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false&user=%s&password=%s",
                        pipelineOptions.getDatabase(), pipelineOptions.getCloudSqlInstance(), pipelineOptions.getUser(), pipelineOptions.getPassword());

        PipelineOptionsFactory.register(JdbcToCsvOptions.class);
        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        JdbcToCsvRowMapper rowMapper = new JdbcToCsvRowMapper(pipelineOptions);

        // reading from MySQL (Cloud SQL works) and encrypting data if it is needed
        final PCollection<String> csvStrings = pipeline.apply(JdbcIO.<String>read()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                "com.mysql.cj.jdbc.Driver", connectionString))
                .withCoder(StringUtf8Coder.of())
                .withQuery(pipelineOptions.getQuery())
                .withRowMapper(rowMapper));

        // writing csv data to the cloud storage bucket
        csvStrings.apply(TextIO.write().to("gs://" + pipelineOptions.getOutputBucket() + "/data_from_jdbc").withSuffix(".enc").withoutSharding());

        // if decryption is needed - reading from cloud storage bucket, decrypting and writing decrypted data back to cloud storage
        if (pipelineOptions.isDecrypt()) {
            pipeline.apply(FileIO.match().filepattern("gs://" + pipelineOptions.getOutputBucket() + "/data_from_jdbc.enc"))
                    .apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
                    .apply(ParDo.of(new DoFn<FileIO.ReadableFile, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext context) throws IOException {
                            final JdbcToCsvOptions options = (JdbcToCsvOptions) context.getPipelineOptions();
                            FileIO.ReadableFile f = context.element();

                            final byte[] encryptedFileContentBytes = f.readFullyAsBytes();
                            final byte[] decryptedFileContentBytes =
                                    Util.decrypt(options.getProject(),
                                            options.getLocation(),
                                            options.getKeyring(),
                                            options.getKey(),
                                            encryptedFileContentBytes);
                            context.output(new String(decryptedFileContentBytes));
                        }
                    })).apply(TextIO.write().to("gs://" + pipelineOptions.getOutputBucket() + "/decrypted_data_from_jdbc").withSuffix(".csv").withoutSharding());

        }

        pipeline.run();
    }



}
