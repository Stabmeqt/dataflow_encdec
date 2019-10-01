package com.epam.dataflow;

import com.epam.dataflow.transform.ReadFromJdbcFn;
import com.epam.dataflow.transform.WriteFileTranform;
import com.epam.dataflow.util.CryptoUtils;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.compress.utils.IOUtils;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class Main {

    public static void main(String[] args) {

        JdbcToCsvOptions pipelineOptions = null;
        try {
            pipelineOptions = PipelineOptionsFactory
                    .fromArgs(args)
                    .withValidation()
                    .as(JdbcToCsvOptions.class);

        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            PipelineOptionsFactory.printHelp(System.err, JdbcToCsvOptions.class);
            System.exit(1);
        }

        pipelineOptions.setAutoscalingAlgorithm(
                DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED);

        PipelineOptionsFactory.register(JdbcToCsvOptions.class);
        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        final String outputBucket = pipelineOptions.getOutputBucket();
        final String keyPath = pipelineOptions.getKeyPath().trim();

        if (pipelineOptions.getDecrypt()) {
            pipeline.apply(FileIO.match().filepattern("gs://" +
                    outputBucket + "/data_from_jdbc*"))
                    .apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
                    .apply("Decrypt", ParDo.of(new DoFn<FileIO.ReadableFile, byte[]>() {
                        @ProcessElement
                        public void processElement(@Element FileIO.ReadableFile input, OutputReceiver<byte[]> out) throws IOException {
                            final byte[] encryptedBytes = input.readFullyAsBytes();
                            try (ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
                                    keyPath, false))) {
                                SecretKeySpec keySpec =
                                        new SecretKeySpec(IOUtils.toByteArray(Channels.newInputStream(chan)), "AES");
                                final byte[] decrypted = CryptoUtils.CISDecrypt(encryptedBytes, keySpec);
                                out.output(decrypted);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    })).apply(new WriteFileTranform(new ByteArraySink(), outputBucket, "decrypted_data_from_jdbc"));
        } else {
            pipeline
                    .apply(new ReadFromJdbcFn(pipelineOptions))
                    .apply("Encrypt", ParDo.of(new DoFn<String, byte[]>() {
                        @ProcessElement
                        public void processElement(@Element String element, OutputReceiver<byte[]> out) {
                            try (ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(keyPath, false))) {
                                SecretKeySpec keySpec =
                                        new SecretKeySpec(IOUtils.toByteArray(Channels.newInputStream(chan)), "AES");
                                final byte[] aes = CryptoUtils.CISEncrypt(element.getBytes("UTF-8"), keySpec);
                                out.output(aes);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    })).apply(new WriteFileTranform(new ByteArraySink(), outputBucket, "data_from_jdbc"));
        }

        pipeline.run();
    }

}
