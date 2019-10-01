package com.epam.dataflow;

import com.google.common.io.ByteStreams;
import org.apache.beam.sdk.io.FileIO;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class ByteArraySink implements FileIO.Sink<byte[]> {

    private WritableByteChannel channel;

    public ByteArraySink() {}

    @Override
    public void open(WritableByteChannel channel) throws IOException {
       this.channel = channel;
    }

    @Override
    public void write(byte[] element) throws IOException {
        channel.write(ByteBuffer.wrap(element));
    }

    @Override
    public void flush() throws IOException {
        if (channel.isOpen()) {
            channel.close();
        }
    }
}
