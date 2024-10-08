package com.skraba.avro.enchiridion.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.skraba.avro.enchiridion.testkit.AvroVersion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

/** Unit tests and helper methods to serialize Avro datum to binary. */
public class SerializeToBytesTest {

  /** Use the given {@link GenericData} model to serialize the datum according to the schema. */
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

  /** Use the default generic data to serialize the datum according to the schema. */
  public static <T> byte[] toBytes(Schema schema, T datum) {
    return toBytes(GenericData.get(), schema, datum);
  }

  /** Serialize the datum according to the schema with a null model. */
  public static <T> byte[] toBytesNull(Schema schema, T datum) {
    // TODO: Should this generally be possible?
    return toBytes(null, schema, datum);
  }

  /**
   * Use the given {@link GenericData} to serialize the byte[] according to the schema, wrapping in
   * the ByteBuffer for the BYTES type.
   */
  public static byte[] toBytesB(GenericData model, Schema schema, byte[] datum) {
    return toBytes(model, schema, ByteBuffer.wrap(datum));
  }

  /**
   * Use the given {@link GenericData} to serialize the byte[] according to the schema, wrapping in
   * the GenericFixed for the FIXED type.
   */
  public static byte[] toBytesF(GenericData model, Schema schema, byte[] datum) {
    return toBytes(model, schema, new GenericData.Fixed(schema, datum));
  }

  /**
   * Use the given {@link GenericData} to deserialize a datum from the bytes according to the
   * schema.
   */
  public static <T> T fromBytes(
      GenericData model, Schema writer, Schema reader, byte[] serialized) {
    Objects.requireNonNull(model);
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      DatumReader<T> r = new GenericDatumReader<>(writer, reader, model);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  /**
   * Use the given {@link GenericData} to deserialize a datum from the bytes according to the
   * schema.
   */
  public static <T> T fromBytes(GenericData model, Schema schema, byte[] serialized) {
    return fromBytes(model, schema, schema, serialized);
  }

  /**
   * Use the given {@link GenericData} to deserialize a datum from the bytes (represented as
   * integers) according to the schema.
   */
  public static <T> T fromBytes(GenericData model, Schema schema, int... serialized) {
    byte[] asBytes = new byte[serialized.length];
    for (int i = 0; i < asBytes.length; i++) {
      asBytes[i] = (byte) serialized[i];
    }
    return fromBytes(model, schema, asBytes);
  }

  public static <T> T fromBytes(Schema writer, Schema reader, byte[] serialized) {
    return fromBytes(GenericData.get(), writer, reader, serialized);
  }

  public static <T> T fromBytes(Schema schema, byte[] serialized) {
    return fromBytes(GenericData.get(), schema, serialized);
  }

  public static <T> T fromBytes(Schema schema, int... serialized) {
    return fromBytes(GenericData.get(), schema, serialized);
  }

  /**
   * Use the given {@link GenericData} to deserialize a datum from the bytes according to the
   * schema.
   */
  public static byte[] fromBytesB(GenericData model, Schema schema, byte[] serialized) {
    return SerializeToBytesTest.<ByteBuffer>fromBytes(model, schema, serialized).array();
  }

  /**
   * Use the given {@link GenericData} to deserialize a fixed datum from the bytes according to the
   * schema.
   */
  public static byte[] fromBytesF(GenericData model, Schema schema, byte[] serialized) {
    return SerializeToBytesTest.<GenericData.Fixed>fromBytes(model, schema, serialized).bytes();
  }

  public static <T> T roundTripBytes(GenericData model, Schema schema, T datum) {
    return fromBytes(model, schema, toBytes(model, schema, datum));
  }

  public static <T> T roundTripBytes(Schema schema, T datum) {
    return roundTripBytes(GenericData.get(), schema, datum);
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
  void testRoundTripFloat() {
    Schema schema = SchemaBuilder.builder().floatType();

    // Around zero
    assertThat(toBytesNull(schema, 0f))
        .containsExactly(0x00, 0x00, 0x00, 0x00)
        .satisfies(
            value -> assertThat((float) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytesNull(schema, -0f))
        .containsExactly(0x00, 0x00, 0x00, 0x80)
        .satisfies(
            value -> assertThat((float) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytesNull(schema, 1f))
        .containsExactly(0x00, 0x00, 0x80, 0x3f)
        .satisfies(
            value -> assertThat((float) fromBytes(GenericData.get(), schema, value)).isOne());
    assertThat(toBytesNull(schema, -1f))
        .containsExactly(0x00, 0x00, 0x80, 0xbf)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value)).isEqualTo(-1f));

    // Special numbers
    assertThat(toBytesNull(schema, Float.POSITIVE_INFINITY))
        .containsExactly(0x00, 0x00, 0x80, 0x7f)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.POSITIVE_INFINITY));
    assertThat(toBytesNull(schema, Float.NEGATIVE_INFINITY))
        .containsExactly(0x00, 0x00, 0x80, 0xff)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.NEGATIVE_INFINITY));
    assertThat(toBytesNull(schema, Float.MAX_VALUE))
        .containsExactly(0xff, 0xff, 0x7f, 0x7f)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.MAX_VALUE));
    assertThat(toBytesNull(schema, Float.MIN_NORMAL))
        .containsExactly(0x00, 0x00, 0x80, 0x00)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.MIN_NORMAL));
    assertThat(toBytesNull(schema, Float.MIN_VALUE))
        .containsExactly(0x01, 0x00, 0x00, 0x00)
        .satisfies(
            value ->
                assertThat((float) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Float.MIN_VALUE));

    // Two different NaN
    assertThat(toBytesNull(schema, Float.NaN))
        .containsExactly(0x00, 0x00, 0xC0, 0x7f)
        .satisfies(
            value -> assertThat((float) fromBytes(GenericData.get(), schema, value)).isNaN());
    assertThat((float) fromBytes(GenericData.get(), schema, 0x12, 0x34, 0xcf, 0x7f)).isNaN();
  }

  @Test
  void testRoundTripDouble() {
    Schema schema = SchemaBuilder.builder().doubleType();

    // Around zero
    assertThat(toBytesNull(schema, 0d))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
        .satisfies(
            value -> assertThat((double) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytesNull(schema, -0d))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80)
        .satisfies(
            value -> assertThat((double) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytesNull(schema, 1d))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f)
        .satisfies(
            value -> assertThat((double) fromBytes(GenericData.get(), schema, value)).isOne());
    assertThat(toBytesNull(schema, -1d))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0xbf)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value)).isEqualTo(-1f));

    // Special numbers
    assertThat(toBytesNull(schema, Double.POSITIVE_INFINITY))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x7f)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.POSITIVE_INFINITY));
    assertThat(toBytesNull(schema, Double.NEGATIVE_INFINITY))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0xff)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.NEGATIVE_INFINITY));
    assertThat(toBytesNull(schema, Double.MAX_VALUE))
        .containsExactly(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.MAX_VALUE));
    assertThat(toBytesNull(schema, Double.MIN_NORMAL))
        .containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.MIN_NORMAL));
    assertThat(toBytesNull(schema, Double.MIN_VALUE))
        .containsExactly(0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
        .satisfies(
            value ->
                assertThat((double) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Double.MIN_VALUE));

    // Two different NaN
    assertThat(toBytesNull(schema, Double.NaN))
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

    // Around zero
    assertThat(toBytesNull(schema, 0))
        .containsExactly(0x00)
        .satisfies(value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytesNull(schema, 1))
        .containsExactly(0x02)
        .satisfies(value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isOne());
    assertThat(toBytesNull(schema, -1))
        .containsExactly(0x01)
        .satisfies(
            value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isEqualTo(-1));

    // The binary representation is 00000110, there is no high bit, so just convert
    // to decimal and divide by two.
    assertThat(toBytesNull(schema, 5))
        .containsExactly(0x0a)
        .satisfies(
            value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isEqualTo(5));
    assertThat((int) fromBytes(GenericData.get(), schema, 0x8a, 0x80, 0x80, 0x80, 0x00))
        .isEqualTo(5);

    assertThat(toBytesNull(schema, 42))
        .containsExactly(0x54)
        .satisfies(
            value -> assertThat((int) fromBytes(GenericData.get(), schema, value)).isEqualTo(42));
    assertThat((int) fromBytes(GenericData.get(), schema, 0xd4, 0x80, 0x80, 0x80, 0x00))
        .isEqualTo(42);

    assertThat(toBytesNull(schema, 1_234_567_890))
        .containsExactly(0xa4, 0x8b, 0xb0, 0x99, 0x09)
        .satisfies(
            value ->
                assertThat((int) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(1_234_567_890));
    assertThat(toBytesNull(schema, -1_234_567_890))
        .containsExactly(0xa3, 0x8b, 0xb0, 0x99, 0x09)
        .satisfies(
            value ->
                assertThat((int) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(-1_234_567_890));

    // Ranges
    assertThat((int) fromBytes(GenericData.get(), schema, 0x7e)).isEqualTo(63);
    assertThat((int) fromBytes(GenericData.get(), schema, 0x7f)).isEqualTo(-64);
    assertThat(toBytesNull(schema, 64)).containsExactly(0x80, 0x01);
    assertThat(toBytesNull(schema, -65)).containsExactly(0x81, 0x01);
    assertThat(toBytesNull(schema, 8191)).containsExactly(0xfe, 0x7f);
    assertThat(toBytesNull(schema, 8192)).containsExactly(0x80, 0x80, 0x01);
    assertThat(toBytesNull(schema, 1048576)).containsExactly(0x80, 0x80, 0x80, 0x01);
    assertThat(toBytesNull(schema, 134217728)).containsExactly(0x80, 0x80, 0x80, 0x80, 0x01);

    assertThat(toBytesNull(schema, Integer.MIN_VALUE))
        .containsExactly(0xff, 0xff, 0xff, 0xff, 0x0f)
        .satisfies(
            value ->
                assertThat((int) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Integer.MIN_VALUE));
    assertThat(toBytesNull(schema, Integer.MAX_VALUE))
        .containsExactly(0xfe, 0xff, 0xff, 0xff, 0x0f)
        .satisfies(
            value ->
                assertThat((int) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Integer.MAX_VALUE));
  }

  @Test
  public void testRoundTripLong() {
    Schema schema = SchemaBuilder.builder().longType();

    // Around zero
    assertThat(toBytesNull(schema, 0L))
        .containsExactly(0x00)
        .satisfies(
            value -> assertThat((long) fromBytes(GenericData.get(), schema, value)).isZero());
    assertThat(toBytesNull(schema, 1L))
        .containsExactly(0x02)
        .satisfies(value -> assertThat((long) fromBytes(GenericData.get(), schema, value)).isOne());
    assertThat(toBytesNull(schema, -1L))
        .containsExactly(0x01)
        .satisfies(
            value -> assertThat((long) fromBytes(GenericData.get(), schema, value)).isEqualTo(-1L));

    // The binary representation is 00000110, there is no high bit, so just convert
    // to decimal and divide by two.
    assertThat(toBytesNull(schema, 5L))
        .containsExactly(0x0a)
        .satisfies(
            value -> assertThat((long) fromBytes(GenericData.get(), schema, value)).isEqualTo(5));
    assertThat((long) fromBytes(GenericData.get(), schema, 0x8a, 0x80, 0x80, 0x80, 0x00))
        .isEqualTo(5L);

    assertThat(toBytesNull(schema, 42L))
        .containsExactly(0x54)
        .satisfies(
            value -> assertThat((long) fromBytes(GenericData.get(), schema, value)).isEqualTo(42L));
    assertThat((long) fromBytes(GenericData.get(), schema, 0xd4, 0x80, 0x80, 0x80, 0x00))
        .isEqualTo(42L);

    // Ranges
    assertThat((long) fromBytes(GenericData.get(), schema, 0x7e)).isEqualTo(63L);
    assertThat((long) fromBytes(GenericData.get(), schema, 0x7f)).isEqualTo(-64L);
    assertThat(toBytesNull(schema, 64L)).containsExactly(0x80, 0x01);
    assertThat(toBytesNull(schema, -65L)).containsExactly(0x81, 0x01);
    assertThat(toBytesNull(schema, 8191L)).containsExactly(0xfe, 0x7f);
    assertThat(toBytesNull(schema, 8192L)).containsExactly(0x80, 0x80, 0x01);
    assertThat(toBytesNull(schema, 1048576L)).containsExactly(0x80, 0x80, 0x80, 0x01);
    assertThat(toBytesNull(schema, 134217728L)).containsExactly(0x80, 0x80, 0x80, 0x80, 0x01);

    assertThat(toBytesNull(schema, Long.MIN_VALUE))
        .containsExactly(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)
        .satisfies(
            value ->
                assertThat((long) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Long.MIN_VALUE));
    assertThat(toBytesNull(schema, Long.MAX_VALUE))
        .containsExactly(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)
        .satisfies(
            value ->
                assertThat((long) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(Long.MAX_VALUE));
  }

  @Test
  public void testRoundTripNull() {
    Schema schema = SchemaBuilder.builder().nullType();

    // From a null to a byte array.
    assertThat(toBytesNull(schema, null))
        .hasSize(0)
        .satisfies(
            value -> assertThat((Object) fromBytes(GenericData.get(), schema, value)).isNull());

    // This might be surprising, any datum can be passed and it will be ignored.
    assertThat(toBytesNull(schema, new Object()))
        .hasSize(0)
        .satisfies(
            value -> assertThat((Object) fromBytes(GenericData.get(), schema, value)).isNull());
  }

  @Test
  public void testRoundTripBytes() {
    Schema schema = SchemaBuilder.builder().bytesType();

    // Use extra helper methods to test with byte arrays.  The actual return value will be a
    // ByteBuffer!
    assertThat(toBytesB(null, schema, new byte[] {}))
        .containsExactly(0x00)
        .satisfies(value -> assertThat(fromBytesB(GenericData.get(), schema, value)).hasSize(0));
    assertThat(toBytesB(null, schema, new byte[] {0x12, 0x34}))
        .containsExactly(0x04, 0x12, 0x34)
        .satisfies(
            value -> {
              assertThat((ByteBuffer) fromBytes(GenericData.get(), schema, value))
                  .satisfies(bb -> assertThat(bb.remaining()).isEqualTo(2));
              assertThat(fromBytesB(GenericData.get(), schema, value)).containsExactly(0x12, 0x34);
            });
  }

  @Test
  public void testRoundTripString() {
    Schema schema = SchemaBuilder.builder().stringType();

    // Note that this is a CharSequence, but the result is Utf8 not Java String.
    assertThat(toBytesNull(schema, ""))
        .containsExactly(0x00)
        .satisfies(
            value ->
                assertThat((CharSequence) fromBytes(GenericData.get(), schema, value))
                    .isInstanceOf(Utf8.class)
                    .isEmpty());
    assertThat(toBytesNull(schema, new Utf8("Hello")))
        .containsExactly(0x0a, 0x48, 0x65, 0x6c, 0x6c, 0x6f)
        .satisfies(
            value ->
                assertThat((CharSequence) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo(new Utf8("Hello")));
  }

  @Test
  public void testRoundTripJavaString() {
    Schema schema =
        SchemaBuilder.builder().stringBuilder().prop("avro.java.string", "String").endString();

    // Note that this is a CharSequence, but the result is Utf8 not Java String.
    assertThat(toBytesNull(schema, ""))
        .containsExactly(0x00)
        .satisfies(
            value -> assertThat((String) fromBytes(GenericData.get(), schema, value)).isEmpty());
    assertThat(toBytesNull(schema, new Utf8("Hello")))
        .containsExactly(0x0a, 0x48, 0x65, 0x6c, 0x6c, 0x6f)
        .satisfies(
            value ->
                assertThat((String) fromBytes(GenericData.get(), schema, value))
                    .isEqualTo("Hello"));
  }

  @Test
  public void testRoundTripArray() {
    Schema schema = SchemaBuilder.builder().array().items().longType();

    // Note that this is a CharSequence, but the result is Utf8 not Java String.
    assertThat(toBytesNull(schema, Arrays.asList(4L, 5L, 6L)))
        .containsExactly(0x06, 0x08, 0x0a, 0x0c, 0x00)
        .satisfies(
            value ->
                assertThat(
                        SerializeToBytesTest.<List<Long>>fromBytes(
                            GenericData.get(), schema, value))
                    .containsExactly(4L, 5L, 6L));

    // These are all equivalent expressions, starting with a three element list:
    assertThat(
            SerializeToBytesTest.<List<Long>>fromBytes(
                GenericData.get(), schema, 0x06, 0x08, 0x0a, 0x0c, 0x00))
        .containsExactly(4L, 5L, 6L);
    // three one element lists:
    assertThat(
            SerializeToBytesTest.<List<Long>>fromBytes(
                GenericData.get(), schema, 0x02, 0x08, 0x02, 0x0a, 0x02, 0x0c, 0x00))
        .containsExactly(4L, 5L, 6L);
    // and three one element lists with the skip size!
    assertThat(
            SerializeToBytesTest.<List<Long>>fromBytes(
                GenericData.get(),
                schema,
                0x01,
                0x02,
                0x08,
                0x01,
                0x02,
                0x0a,
                0x01,
                0x02,
                0x0c,
                0x00))
        .containsExactly(4L, 5L, 6L);
  }

  @Test
  public void testRoundTripMap() {
    Schema schema = SchemaBuilder.builder().map().values().longType();

    Map<String, Long> datum = new HashMap<>();
    datum.put("Hello", 4L);
    datum.put("Bye", 5L);

    // These are written as functions here to help with the autoformatting and the long byte arrays.
    Function<byte[], Map<CharSequence, Long>> de =
        value -> fromBytes(GenericData.get(), schema, value);
    Function<int[], Map<CharSequence, Long>> de2 =
        value -> fromBytes(GenericData.get(), schema, value);

    // Note that this is a CharSequence, but the result is Utf8 not Java String.
    assertThat(toBytesNull(schema, datum))
        .containsExactly(
            0x04, 0x0a, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x08, 0x06, 0x42, 0x79, 0x65, 0x0a, 0x00)
        .satisfies(
            value ->
                assertThat(de.apply(value))
                    .hasSize(2)
                    .containsEntry(new Utf8("Hello"), 4L)
                    .containsEntry(new Utf8("Bye"), 5L));

    // These are all equivalent expressions, starting with a 2 element list:
    assertThat(
            de2.apply(
                new int[] {
                  0x04, 0x0a, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x08, 0x06, 0x42, 0x79, 0x65, 0x0a, 0x00
                }))
        .hasSize(2)
        .containsEntry(new Utf8("Hello"), 4L)
        .containsEntry(new Utf8("Bye"), 5L);
    // Keys in the other order!
    assertThat(
            de2.apply(
                new int[] {
                  0x04, 0x06, 0x42, 0x79, 0x65, 0x0a, 0x0a, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x08, 0x00
                }))
        .hasSize(2)
        .containsEntry(new Utf8("Hello"), 4L)
        .containsEntry(new Utf8("Bye"), 5L);
    // and two one element lists
    assertThat(
            de2.apply(
                new int[] {
                  0x02, 0x0a, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x08, 0x02, 0x06, 0x42, 0x79, 0x65,
                  0x0a, 0x00
                }))
        .hasSize(2)
        .containsEntry(new Utf8("Hello"), 4L)
        .containsEntry(new Utf8("Bye"), 5L);
    // and two one element lists with the skip size!
    assertThat(
            de2.apply(
                new int[] {
                  0x01, 0x10, 0x0a, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x08, 0x01, 0x1a, 0x06, 0x42,
                  0x79, 0x65, 0x0a, 0x00
                }))
        .hasSize(2)
        .containsEntry(new Utf8("Hello"), 4L)
        .containsEntry(new Utf8("Bye"), 5L);
  }

  @Test
  public void testRoundTripFixed() {
    Schema schemaF0 = SchemaBuilder.builder().fixed("F0").size(0);
    Schema schemaF2 = SchemaBuilder.builder().fixed("F2").size(2);

    // Use extra helper methods to test with byte arrays.  The actual return value will be a
    // GenericFixed!
    assertThat(toBytesF(null, schemaF0, new byte[] {}))
        .hasSize(0)
        .satisfies(value -> assertThat(fromBytesF(GenericData.get(), schemaF0, value)).hasSize(0));
    assertThat(toBytesF(null, schemaF2, new byte[] {0x12, 0x34}))
        .containsExactly(0x12, 0x34)
        .satisfies(
            value -> {
              assertThat((GenericFixed) fromBytes(GenericData.get(), schemaF2, value))
                  .satisfies(gf -> assertThat(gf.bytes()).containsExactly(0x12, 0x34));
              assertThat(fromBytesF(GenericData.get(), schemaF2, value))
                  .containsExactly(0x12, 0x34);
            });
  }

  @Test
  public void testRoundTripEnum() {
    Schema schema = SchemaBuilder.builder().enumeration("E1").symbols("Z", "Y", "X", "W");

    // TODO: This doesn't always need to be set for others, why does it need to be set here?
    assertThat(toBytes(schema, new GenericData.EnumSymbol(schema, "Z")))
        .containsExactly(0x00)
        .satisfies(
            value ->
                assertThat((GenericEnumSymbol<?>) fromBytes(GenericData.get(), schema, value))
                    .hasToString("Z"));
    assertThat(toBytes(schema, new GenericData.EnumSymbol(schema, "W")))
        .containsExactly(0x06)
        .satisfies(
            value ->
                assertThat((GenericEnumSymbol<?>) fromBytes(GenericData.get(), schema, value))
                    .hasToString("W"));

    if (AvroVersion.avro_1_12.orAfter("Changed exception type in 1.12.0")) {
      assertThatThrownBy(() -> toBytes(schema, new GenericData.EnumSymbol(schema, "A")))
          .isInstanceOf(AvroTypeException.class)
          .hasMessageContaining("enum value 'A' is not in the enum symbol set: [Z, Y, X, W]");
    } else {
      assertThatThrownBy(() -> toBytes(schema, new GenericData.EnumSymbol(schema, "A")))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining(
              AvroVersion.avro_1_11.before("Changed exception message in 1.11.1")
                  ? "null of E1"
                  : "null value for (non-nullable) E1");
    }
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
