package com.skraba.avro.enchiridion.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.junit.EnabledForAvroVersion;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.MessageEncoder;
import org.junit.jupiter.api.Test;

/**
 * Unit tests and helper methods to serialize Avro datum to single object encoding.
 *
 * @see <a href="https://avro.apache.org/docs/current/spec.html#single_object_encoding">Single
 *     Object Encoding</a>
 */
@EnabledForAvroVersion(
    startingFrom = AvroVersion.avro_1_8,
    reason = "The single object encoding message format was introduced in 1.8.")
public class SerializeToMessageTest {

  public static <T> byte[] toMessage(GenericData model, Schema schema, T datum) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      MessageEncoder<T> encoder = new BinaryMessageEncoder<>(model, schema);
      encoder.encode(datum, baos);
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T fromMessage(GenericData model, Schema schema, byte[] serialized) {
    try {
      MessageDecoder<T> decoder = new BinaryMessageDecoder<>(model, schema);
      return decoder.decode(serialized);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T roundTripMessage(GenericData model, Schema schema, T datum) {
    return fromMessage(model, schema, toMessage(model, schema, datum));
  }

  @Test
  public void testRoundTripSerializeIntegerToMessage() {
    Schema schema = SchemaBuilder.builder().intType();

    // From an int to a byte array.
    byte[] serialized = toMessage(GenericData.get(), schema, 1_234_567);
    assertThat(serialized.length, is(14));

    // Check some of the info in the message header.
    assertThat(serialized[0] & 0xFF, is(0xc3));
    assertThat(serialized[1] & 0xFF, is(0x01));
    assertThat(
        Long.toHexString(SchemaNormalization.parsingFingerprint64(schema)), is("7275d51a3f395c8f"));
    assertThat(serialized[2] & 0xFF, is(0x8f));
    assertThat(serialized[3] & 0xFF, is(0x5c));
    assertThat(serialized[4] & 0xFF, is(0x39));
    assertThat(serialized[5] & 0xFF, is(0x3f));
    // and so on...

    Integer datum = fromMessage(GenericData.get(), schema, serialized);
    assertThat(datum, is(1_234_567));

    assertThat(roundTripMessage(GenericData.get(), schema, 1), is(1));
    assertThat(roundTripMessage(GenericData.get(), schema, 0), is(0));
    assertThat(roundTripMessage(GenericData.get(), schema, -1), is(-1));
  }
}
