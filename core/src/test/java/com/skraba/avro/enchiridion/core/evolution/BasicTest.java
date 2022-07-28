package com.skraba.avro.enchiridion.core.evolution;

import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

/** Basic tests for round-trip serialization with different reader and writer schemas. */
public class BasicTest {

  /** A simple, original schema. */
  public static final Schema SIMPLE_V1 =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .requiredString("name")
          .endRecord();

  /** An example record for {@link #SIMPLE_V1}. */
  public static final GenericRecord RECORD_V1 =
      new GenericRecordBuilder(SIMPLE_V1).set("id", 1L).set("name", "one").build();

  /** {@link #RECORD_V1} represented as binary. */
  public static final byte[] BINARY_V1 = toBytes(SIMPLE_V1, RECORD_V1);

  /** The same as the original schema but with a new field. */
  private static final Schema SIMPLE_V2 =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .requiredString("name")
          .name("rating")
          .type()
          .floatType()
          .floatDefault(2.5f) // New fields must have a default.
          .endRecord();

  /** The same as the original schema but with a new field. */
  private static final Schema SIMPLE_V2_MISSING_DEFAULT =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .requiredString("name")
          .name("rating")
          .type()
          .floatType()
          .noDefault() // It's an error to be missing the default.
          .endRecord();

  /**
   * A clear example evolving one schema into another using binary serialization.
   *
   * @param original The original datum.
   * @param actual The schema for the original datum (should match original.getSchema() if that
   *     exists.
   * @param expected The expected or desired schema after deserialization.
   * @return The datum with the expected or desired schema.
   */
  public static <In, Out> Out evolveUsingBinarySerialization(
      In original, Schema actual, Schema expected) {
    byte[] binary = null;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<In> w = new GenericDatumWriter<>(actual, GenericData.get());
      w.write(original, encoder);
      encoder.flush();
      binary = baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    // Check that schema resolution is OK by reading with the new schema.
    try (ByteArrayInputStream bais = new ByteArrayInputStream(binary)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      GenericDatumReader<Out> r = new GenericDatumReader<>(actual, expected, GenericData.get());
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * A clear example evolving one schema into another using JSON serialization.
   *
   * @param original The original datum.
   * @param actual The schema for the original datum (should match original.getSchema() if that
   *     exists.
   * @param expected The expected or desired schema after deserialization.
   * @return The datum with the expected or desired schema.
   */
  public static <In, Out> Out evolveUsingJsonSerialization(
      In original, Schema actual, Schema expected) {
    byte[] binary = null;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().jsonEncoder(actual, baos);
      DatumWriter<In> w = new GenericDatumWriter<>(actual, GenericData.get());
      w.write(original, encoder);
      encoder.flush();
      binary = baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    // Check that schema resolution is OK by reading with the new schema.
    try (ByteArrayInputStream bais = new ByteArrayInputStream(binary)) {
      Decoder decoder = DecoderFactory.get().jsonDecoder(actual, bais);
      GenericDatumReader<Out> r = new GenericDatumReader<>(actual, expected, GenericData.get());
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Test
  public void testBinaryEvolution() {
    IndexedRecord evolved = evolveUsingBinarySerialization(RECORD_V1, SIMPLE_V1, SIMPLE_V2);
    assertThat(
        evolved,
        is(
            new GenericRecordBuilder(SIMPLE_V2)
                .set("id", 1L)
                .set("name", "one")
                .set("rating", 2.5f)
                .build()));
  }

  @Test
  public void testBinaryIncompatible() {
    AvroTypeException ex =
        assertThrows(
            AvroTypeException.class,
            () -> evolveUsingBinarySerialization(RECORD_V1, SIMPLE_V1, SIMPLE_V2_MISSING_DEFAULT));
    assertThat(ex.getMessage(), containsString("missing required field rating"));
  }

  @Test
  public void testJsonEvolution() {
    IndexedRecord evolved = evolveUsingJsonSerialization(RECORD_V1, SIMPLE_V1, SIMPLE_V2);
    assertThat(
        evolved,
        is(
            new GenericRecordBuilder(SIMPLE_V2)
                .set("id", 1L)
                .set("name", "one")
                .set("rating", 2.5f)
                .build()));
  }

  @Test
  public void testJsonIncompatible() {
    AvroTypeException ex =
        assertThrows(
            AvroTypeException.class,
            () -> evolveUsingJsonSerialization(RECORD_V1, SIMPLE_V1, SIMPLE_V2_MISSING_DEFAULT));
    assertThat(ex.getMessage(), containsString("missing required field rating"));
  }
}
