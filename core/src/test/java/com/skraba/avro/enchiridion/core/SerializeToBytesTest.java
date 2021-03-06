package com.skraba.avro.enchiridion.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

/** Unit tests and helper methods to serialize Avro datum to binary. */
public class SerializeToBytesTest {

  public static <T> byte[] toBytes(GenericData model, Schema schema, T datum) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<T> w = new GenericDatumWriter<>(schema, model);
      w.write(datum, encoder);
      encoder.flush();
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T fromBytes(GenericData model, Schema schema, byte[] serialized) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      DatumReader<T> r = new GenericDatumReader<>(schema, schema, model);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T roundTripBytes(GenericData model, Schema schema, T datum) {
    return fromBytes(model, schema, toBytes(model, schema, datum));
  }

  public static <T> List<ByteBuffer> toByteBuffers(GenericData model, Schema schema, T datum) {
    try (ByteBufferOutputStream bbos = new ByteBufferOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(bbos, null);
      DatumWriter<T> w = new GenericDatumWriter<>(schema, model);
      w.write(datum, encoder);
      encoder.flush();
      return bbos.getBufferList();
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T fromByteBuffers(
      GenericData model, Schema schema, List<ByteBuffer> serialized) {
    try (ByteBufferInputStream bais = new ByteBufferInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      DatumReader<T> r = new GenericDatumReader<>(schema, schema, model);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T roundTripByteBuffers(GenericData model, Schema schema, T datum) {
    return fromByteBuffers(model, schema, toByteBuffers(model, schema, datum));
  }

  @Test
  public void testRoundTripSerializeIntegerToByteArray() {
    Schema schema = SchemaBuilder.builder().intType();

    // From an int to a byte array.
    byte[] serialized = toBytes(null, schema, 1_234_567);
    assertThat(serialized.length, is(4));

    Integer datum = fromBytes(GenericData.get(), schema, serialized);
    assertThat(datum, is(1_234_567));

    assertThat(roundTripBytes(GenericData.get(), schema, 1), is(1));
    assertThat(roundTripBytes(GenericData.get(), schema, 0), is(0));
    assertThat(roundTripBytes(GenericData.get(), schema, -1), is(-1));
  }

  @Test
  public void testRoundTripSerializeIntegerToByteBuffers() {
    Schema schema = SchemaBuilder.builder().intType();

    // From an int to a byte array.
    List<ByteBuffer> serialized = toByteBuffers(null, schema, 1_234_567);
    assertThat(serialized.size(), is(1));

    Integer datum = fromByteBuffers(GenericData.get(), schema, serialized);
    assertThat(datum, is(1_234_567));

    assertThat(roundTripByteBuffers(GenericData.get(), schema, 1), is(1));
    assertThat(roundTripByteBuffers(GenericData.get(), schema, 0), is(0));
    assertThat(roundTripByteBuffers(GenericData.get(), schema, -1), is(-1));
  }
}
