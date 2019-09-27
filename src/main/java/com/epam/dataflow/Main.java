package com.epam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

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

        PipelineOptionsFactory.register(JdbcToCsvOptions.class);
        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        // reading from JDBC
        final PCollection<String> csvStrings = pipeline.apply(new ReadFromJdbcFn(pipelineOptions));

        // writing csv data to the cloud storage bucket
        csvStrings.apply(TextIO.write().to("gs://" + pipelineOptions.getOutputBucket() + "/data_from_jdbc")
                .withSuffix(".csv").withoutSharding());

        pipeline.run();
    }

}
