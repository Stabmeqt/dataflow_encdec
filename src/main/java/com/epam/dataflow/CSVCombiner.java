package com.epam.dataflow;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.UnsupportedEncodingException;

public class CSVCombiner extends Combine.CombineFn<String, CSVCombiner.Accum, byte[]> {

    @DefaultCoder(AvroCoder.class)
    public static class Accum {
        String content = "";
    }

    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Accum addInput(Accum mutableAccumulator, String input) {
        mutableAccumulator.content += input;
        return mutableAccumulator;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accumulators) {
        Accum merged = createAccumulator();
        for (Accum accumulator : accumulators) {
            merged.content += accumulator.content;
        }
        return merged;
    }

    @Override
    public byte[] extractOutput(Accum accumulator) {
        try {
            final byte[] bytes = accumulator.content.getBytes("UTF-8");
            return bytes;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
