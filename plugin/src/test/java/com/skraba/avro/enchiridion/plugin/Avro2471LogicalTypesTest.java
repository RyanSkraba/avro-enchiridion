package com.skraba.avro.enchiridion.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.idl.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

public class Avro2471LogicalTypesTest {
  public static final Instant TS = Instant.EPOCH;

  public static <T> byte[] toBytes(SpecificData model, Schema schema, T datum) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<T> w = new SpecificDatumWriter<T>(schema, model);
      w.write(datum, encoder);
      encoder.flush();
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T fromBytes(SpecificData model, Schema schema, byte[] serialized) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      DatumReader<T> r = new SpecificDatumReader<T>(schema, schema, model);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  @Test
  public void testTimestampMillisRequired() throws IOException {
    TimestampMillisRequired f = TimestampMillisRequired.newBuilder().setTs(TS).build();
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(4));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  @Test
  public void testTimestampMillisOptional() throws IOException {
    TimestampMillisOptional f = TimestampMillisOptional.newBuilder().setTs(TS).build();
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(5));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  @Test
  public void testTimestampMicrosRequired() throws IOException {
    TimestampMicrosRequired f = TimestampMicrosRequired.newBuilder().setTs(TS).build();
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(4));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  @Test
  public void testTimestampMicrosOptional() throws IOException {
    TimestampMicrosOptional f = TimestampMicrosOptional.newBuilder().setTs(TS).build();
    f.getSpecificData().addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(5));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  //  @Test
  //  public void foobar2x() throws IOException {
  //    final Foobar2 f = Foobar2.newBuilder().setId("aaa").setTs(Instant.now()).build();
  //    f.getSpecificData().addLogicalTypeConversion(new
  // TimeConversions.TimestampMicrosConversion());
  //    byte[] serialized;
  //    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
  //      SpecificDatumWriter<SpecificRecord> writer =
  //          new SpecificDatumWriter<>(((SpecificRecord) f).getSchema());
  //      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
  //      writer.write(f, encoder);
  //      serialized = out.toByteArray();
  //    }
  //
  //    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
  //      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
  //      SpecificDatumReader<Foobar2> r = new SpecificDatumReader<>(Foobar2.class);
  //      Foobar2 floopy = r.read(null, decoder);
  //    }
  //  }
}
