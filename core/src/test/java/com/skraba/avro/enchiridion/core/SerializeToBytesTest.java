package com.skraba.avro.enchiridion.core;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.avro.io.*;
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

  public static <T> T fromBytes(GenericData model, Schema schema, int... serialized) {
    byte[] asBytes = new byte[serialized.length];
    for (int i = 0; i < asBytes.length; i++) {
      asBytes[i] = (byte) serialized[i];
    }
    return fromBytes(model, schema, asBytes);
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
    assertThat(serialized).hasSize(4);

    Integer datum = fromBytes(GenericData.get(), schema, serialized);
    assertThat(datum).isEqualTo(1_234_567);

    assertThat(roundTripBytes(GenericData.get(), schema, 1)).isOne();
    assertThat(roundTripBytes(GenericData.get(), schema, 0)).isZero();
    assertThat(roundTripBytes(GenericData.get(), schema, -1)).isEqualTo(-1);
  }

  @Test
  void testRoundTripSerializeFloat() {
    Schema schema = SchemaBuilder.builder().floatType();

    assertThat(toBytes(null, schema, 0f))
        .containsExactly(0x00, 0x00, 0x00, 0x00)
        .satisfies(
            value -> assertThat((float) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytes(null, schema, -0f))
        .containsExactly(0x00, 0x00, 0x00, 0x80)
        .satisfies(
            value -> assertThat((float) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytes(null, schema, 1f))
        .containsExactly(0x00, 0x00, 0x80, 0x3f)
        .satisfies(
            value -> assertThat((float) fromBytes(GenericData.get(), schema, value)).isOne());
    assertThat(toBytes(null, schema, -1f))
        .containsExactly(0x00, 0x00, 0x80, 0xbf)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value)).isEqualTo(-1f));
    assertThat(toBytes(null, schema, Float.POSITIVE_INFINITY))
        .containsExactly(0x00, 0x00, 0x80, 0x7f)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.POSITIVE_INFINITY));
    assertThat(toBytes(null, schema, Float.NEGATIVE_INFINITY))
        .containsExactly(0x00, 0x00, 0x80, 0xff)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.NEGATIVE_INFINITY));
    assertThat(toBytes(null, schema, Float.MAX_VALUE))
        .containsExactly(0xff, 0xff, 0x7f, 0x7f)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.MAX_VALUE));
    assertThat(toBytes(null, schema, Float.MIN_NORMAL))
        .containsExactly(0x00, 0x00, 0x80, 0x00)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.MIN_NORMAL));
    assertThat(toBytes(null, schema, Float.MIN_VALUE))
        .containsExactly(0x01, 0x00, 0x00, 0x00)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.MIN_VALUE));

    assertThat(toBytes(null, schema, Float.NaN))
        .containsExactly(0x00, 0x00, 0xC0, 0x7f)
        .satisfies(
            value -> assertThat((float) fromBytes(GenericData.get(), schema, value)).isNaN());
    assertThat((float) fromBytes(GenericData.get(), schema, 0x12, 0x34, 0xcf, 0x7f)).isNaN();
  }

  @Test
  void testRoundTripSerializeDouble() {
    Schema schema = SchemaBuilder.builder().doubleType();

    assertThat(toBytes(null, schema, 0d))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
        .satisfies(
            value -> assertThat((double) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytes(null, schema, -0d))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80)
        .satisfies(
            value -> assertThat((double) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytes(null, schema, 1d))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f)
        .satisfies(
            value -> assertThat((double) fromBytes(GenericData.get(), schema, value)).isOne());
    assertThat(toBytes(null, schema, -1d))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0xbf)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value)).isEqualTo(-1f));
    assertThat(toBytes(null, schema, Double.POSITIVE_INFINITY))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x7f)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.POSITIVE_INFINITY));
    assertThat(toBytes(null, schema, Double.NEGATIVE_INFINITY))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0xff)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.NEGATIVE_INFINITY));
    assertThat(toBytes(null, schema, Double.MAX_VALUE))
        .containsExactly(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.MAX_VALUE));
    assertThat(toBytes(null, schema, Double.MIN_NORMAL))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.MIN_NORMAL));
    assertThat(toBytes(null, schema, Double.MIN_VALUE))
        .containsExactly(0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.MIN_VALUE));

    assertThat(toBytes(null, schema, Double.NaN))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f)
        .satisfies(
            value -> assertThat((double) fromBytes(GenericData.get(), schema, value)).isNaN());
    assertThat(
            (double)
                fromBytes(
                    GenericData.get(), schema, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xf8, 0x7f))
        .isNaN();
  }

  @Test
  public void testRoundTripInt() {
    Schema schema = SchemaBuilder.builder().intType();

    // From an int to a byte array.
    assertThat(toBytes(null, schema, 0))
        .containsExactly(0x00)
        .satisfies(value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytes(null, schema, 1))
        .containsExactly(0x02)
        .satisfies(value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isOne());
    assertThat(toBytes(null, schema, -1))
        .containsExactly(0x01)
        .satisfies(
            value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isEqualTo(-1));

    // The binary representation is 00000110, there is no high bit, so just convert
    // to decimal and divide by two.
    assertThat(toBytes(null, schema, 5))
        .containsExactly(0x0a)
        .satisfies(
            value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isEqualTo(5));
    assertThat((int) fromBytes(GenericData.get(), schema, 0x8a, 0x80, 0x80, 0x80, 0x00))
        .isEqualTo(5);

    assertThat(toBytes(null, schema, 42))
        .containsExactly(0x54)
        .satisfies(
            value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isEqualTo(42));
    assertThat((int) fromBytes(GenericData.get(), schema, 0xd4, 0x80, 0x80, 0x80, 0x00))
        .isEqualTo(42);

    assertThat((int) fromBytes(GenericData.get(), schema, 0x7e)).isEqualTo(63);
    assertThat((int) fromBytes(GenericData.get(), schema, 0x7f)).isEqualTo(-64);
    assertThat(toBytes(null, schema, 64)).containsExactly(0x80, 0x01);
    assertThat(toBytes(null, schema, -65)).containsExactly(0x81, 0x01);
    assertThat(toBytes(null, schema, 8191)).containsExactly(0xfe, 0x7f);
    assertThat(toBytes(null, schema, 8192)).containsExactly(0x80, 0x80, 0x01);
    assertThat(toBytes(null, schema, 1048576)).containsExactly(0x80, 0x80, 0x80, 0x01);
    assertThat(toBytes(null, schema, 134217728)).containsExactly(0x80, 0x80, 0x80, 0x80, 0x01);

    assertThat(toBytes(null, schema, Integer.MIN_VALUE))
        .containsExactly(0xff, 0xff, 0xff, 0xff, 0x0f)
        .satisfies(
            value ->
                assertThat((int) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Integer.MIN_VALUE));
    assertThat(toBytes(null, schema, Integer.MAX_VALUE))
        .containsExactly(0xfe, 0xff, 0xff, 0xff, 0x0f)
        .satisfies(
            value ->
                assertThat((int) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Integer.MAX_VALUE));
  }

  @Test
  public void testRoundTripSerializeIntegerToByteBuffers() {
    Schema schema = SchemaBuilder.builder().intType();

    // From an int to a byte array.
    List<ByteBuffer> serialized = toByteBuffers(null, schema, 1_234_567);
    assertThat(serialized).hasSize(1);

    Integer datum = fromByteBuffers(GenericData.get(), schema, serialized);
    assertThat(datum).isEqualTo(1_234_567);

    assertThat(roundTripByteBuffers(GenericData.get(), schema, 1)).isOne();
    assertThat(roundTripByteBuffers(GenericData.get(), schema, 0)).isZero();
    assertThat(roundTripByteBuffers(GenericData.get(), schema, -1)).isEqualTo(-1);
  }
}
