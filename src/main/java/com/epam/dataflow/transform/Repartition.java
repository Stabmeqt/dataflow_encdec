package com.epam.dataflow.transform;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.concurrent.ThreadLocalRandom;

public class Repartition<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private Repartition() {
    }

    public static <T> Repartition<T> of() {
        return new Repartition<T>();
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input
                .apply("Add arbitrary keys", ParDo.of(new AddArbitraryKey<T>()))
                .apply(Reshuffle.of())
                .apply("Remove arbitrary keys", ParDo.of(new RemoveArbitraryKey<T>()));
    }

    public static class AddArbitraryKey<T> extends DoFn<T, KV<Integer, T>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            c.output(KV.of(ThreadLocalRandom.current().nextInt(), c.element()));
        }
    }

    public static class RemoveArbitraryKey<T> extends DoFn<KV<Integer, T>, T> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            c.output(c.element().getValue());
        }
    }
}
