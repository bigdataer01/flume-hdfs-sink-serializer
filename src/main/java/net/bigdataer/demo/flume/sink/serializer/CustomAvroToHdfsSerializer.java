package net.bigdataer.demo.flume.sink.serializer;

import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by liuxuecheng on 2018/7/2.
 *
 * @58corp
 */
public class CustomAvroToHdfsSerializer extends AbstractAvroEventSerializer<RawData> {

    private static final Schema SCHEMA = (new Schema.Parser()).parse("{\"type\":\"record\",\"name\":\"RawData\",\"fields\":[{\"name\":\"body\",\"type\":\"bytes\"}]}");
    private final OutputStream out;

    private CustomAvroToHdfsSerializer(OutputStream out) {
        this.out = out;
    }

    protected Schema getSchema() {
        return SCHEMA;
    }

    protected OutputStream getOutputStream() {
        return this.out;
    }

    protected RawData convert(Event event) {
        /**
         * remove the header of the avro_event
         * when event write to hdfs,it's sequencefile which formatter is <LongWritable,BytesWritable>
         */
        return RawData.newBuilder().setBody(ByteBuffer.wrap(event.getBody())).build();
    }

    public static class Builder implements EventSerializer.Builder {
        public Builder() {
        }

        public EventSerializer build(Context context, OutputStream out) {
            CustomAvroToHdfsSerializer writer = new CustomAvroToHdfsSerializer(out);
            writer.configure(context);
            return writer;
        }
    }
}
