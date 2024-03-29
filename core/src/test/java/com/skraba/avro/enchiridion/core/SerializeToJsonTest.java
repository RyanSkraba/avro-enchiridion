package com.skraba.avro.enchiridion.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.testkit.AvroVersion;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

/** Unit tests and helper methods to serialize Avro datum to JSON. */
public class SerializeToJsonTest {

  public static <T> String toJson(GenericData model, Schema schema, T datum) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().jsonEncoder(schema, baos, false);
      DatumWriter<T> w = new GenericDatumWriter<>(schema, model);
      w.write(datum, encoder);
      encoder.flush();
      return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T fromJson(GenericData model, Schema schema, String serialized) {
    try {
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, serialized);
      DatumReader<T> r = new GenericDatumReader<T>(schema, schema, model);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T roundTripJson(GenericData model, Schema schema, T datum) {
    return fromJson(model, schema, toJson(model, schema, datum));
  }

  @Test
  public void testRoundTripSerializeIntegerToJson() {
    Schema schema = SchemaBuilder.builder().intType();

    // From an int to a String.
    String serialized = toJson(null, schema, 1_234_567);
    assertThat(serialized.length(), is(7));

    Integer datum = fromJson(GenericData.get(), schema, serialized);
    assertThat(datum, is(1_234_567));

    assertThat(roundTripJson(GenericData.get(), schema, 1), is(1));
    assertThat(roundTripJson(GenericData.get(), schema, 0), is(0));
    assertThat(roundTripJson(GenericData.get(), schema, -1), is(-1));
  }

  @Test
  public void testRoundTripSerializeFixedToJson() {
    Schema schema = SchemaBuilder.builder().fixed("f4").size(4);
    GenericFixed f4Datum = new GenericData.Fixed(schema, new byte[] {0x10, 0x20, 0x30, 0x40});

    // From an fixed datum to a String.
    String serialized = toJson(null, schema, f4Datum);
    assertThat(serialized, is("\"\\u0010 0@\""));
    assertThat(serialized.length(), is(11));

    // And from that String to a datum.
    GenericFixed datum = fromJson(GenericData.get(), schema, serialized);
    assertThat(datum, is(f4Datum));

    // There are other valid JSON representations of that data.
    assertThat(
        fromJson(GenericData.get(), schema, "\"\\u0010\\u0020\\u0030\\u0040\""), is(f4Datum));
    assertThat(fromJson(GenericData.get(), schema, "\"\\u0010 \\u0030\\u0040\""), is(f4Datum));
    assertThat(fromJson(GenericData.get(), schema, "\"\\u0010\\u00200\\u0040\""), is(f4Datum));
    assertThat(fromJson(GenericData.get(), schema, "\"\\u0010\\u0020\\u0030@\""), is(f4Datum));
  }

  @Test
  public void testRoundTripSerializeDoubleToJson() {
    Schema schema = SchemaBuilder.builder().doubleType();

    // From an int to a byte array.
    String serialized = toJson(null, schema, 1.25);
    assertThat(serialized.length(), is(4));

    Double datum = fromJson(GenericData.get(), schema, serialized);
    assertThat(datum, is(1.25));

    assertThat(roundTripJson(GenericData.get(), schema, 1.0), is(1.0));
    assertThat(roundTripJson(GenericData.get(), schema, 0.0), is(0.0));
    assertThat(roundTripJson(GenericData.get(), schema, -1.0), is(-1.0));

    // Numeric conversions are only supported after Avro 1.10
    if (AvroVersion.avro_1_10.orAfter()) {
      assertThat(roundTripJson(GenericData.get(), schema, (Number) 1), is(1.0));
      assertThat(roundTripJson(GenericData.get(), schema, (Number) 0), is(0.0));
      assertThat(roundTripJson(GenericData.get(), schema, (Number) (-1)), is(-1.0));
    }

    // These can be encoded by all versions.
    String nan = toJson(GenericData.get(), schema, Double.NaN);
    assertThat(nan, is("\"NaN\""));
    String infPos = toJson(GenericData.get(), schema, Double.POSITIVE_INFINITY);
    assertThat(infPos, is("\"Infinity\""));
    String infNeg = toJson(GenericData.get(), schema, Double.NEGATIVE_INFINITY);
    assertThat(infNeg, is("\"-Infinity\""));

    // But nobody can decode them yet.
    if (AvroVersion.avro_infinity.orAfter()) {
      assertThat(fromJson(GenericData.get(), schema, nan), is(Double.NaN));
      assertThat(fromJson(GenericData.get(), schema, infPos), is(Double.POSITIVE_INFINITY));
      assertThat(fromJson(GenericData.get(), schema, infNeg), is(Double.NEGATIVE_INFINITY));
    }
  }
}
