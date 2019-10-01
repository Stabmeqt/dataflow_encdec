package com.epam.dataflow.transform;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

public class WriteFileTranform extends PTransform<PCollection<byte[]>, POutput> {

    private FileIO.Sink sink;
    private String outputBucket;
    private String fileNamePreffix;

    public WriteFileTranform(FileIO.Sink sink, String outputBucket, String fileNamePreffix) {
        this.sink = sink;
        this.outputBucket = outputBucket;
        this.fileNamePreffix = fileNamePreffix;
    }

    @Override
    public POutput expand(PCollection<byte[]> input) {
        return input.apply(FileIO.write()
                .via(sink)
                .to("gs://" + outputBucket)
                .withNaming(FileIO.Write.defaultNaming(fileNamePreffix, ".csv"))
                .withCompression(Compression.UNCOMPRESSED));
    }
}
