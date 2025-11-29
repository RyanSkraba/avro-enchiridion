package com.skraba.avro.enchiridion.core.evolution;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.skraba.avro.enchiridion.testkit.AvroVersion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test reading data with a schema that has evolved by widening a primitive.
 *
 * <p>The following promotions are permitted:
 *
 * <ol>
 *   <li><b>int</b> is promotable to long, float, or double
 *   <li><b>long</b> is promotable to float or double (although this can lose precision)
 *   <li><b>float</b> is promotable to double
 *   <li><b>string</b> is promotable to bytes
 *   <li><b>bytes</b> is promotable to string
 * </ol>
 */
class EvolveWidenPrimitivesWithLogicalTypesTest extends EvolveWidenPrimitivesTest {

  /**
   * A Stream of all evolutions possible between the schemas in ALL, as well as whether or not the
   * evolution should be permitted.
   */
  public Stream<Schema> getAllSchemasToCrossCheck() {
    return Stream.concat(
        super.getAllSchemasToCrossCheck(),
        Stream.of(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))));
  }

  private static final Schema INT = Schema.create(Schema.Type.INT);
  private static final Schema U_INT = api().createUnion(INT);
  private static final Schema UN_INT = api().createUnion(Schema.create(Schema.Type.NULL), INT);
  private static final Schema DATE =
      LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
  private static final Schema U_DATE = api().createUnion(DATE);
  private static final Schema UN_DATE = api().createUnion(Schema.create(Schema.Type.NULL), DATE);
  private static final Schema LONG = Schema.create(Schema.Type.LONG);
  private static final Schema U_LONG = api().createUnion(LONG);
  private static final Schema UN_LONG = api().createUnion(Schema.create(Schema.Type.NULL), LONG);
  private static final Schema TIMEMICROS =
      LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema U_TIMEMICROS = api().createUnion(TIMEMICROS);
  private static final Schema UN_TIMEMICROS =
      api().createUnion(Schema.create(Schema.Type.NULL), TIMEMICROS);

  private static final byte[] SERIALIZED_1000 = toBytes(api().withTimeConversions(), INT, 1000);
  private static final byte[] SERIALIZED_U_1000 = toBytes(api().withTimeConversions(), U_INT, 1000);
  private static final byte[] SERIALIZED_UN_1000 =
      toBytes(api().withTimeConversions(), UN_INT, 1000);
  private static final byte[] SERIALIZED_1000L = toBytes(api().withTimeConversions(), LONG, 1000L);
  private static final byte[] SERIALIZED_U_1000L =
      toBytes(api().withTimeConversions(), U_LONG, 1000L);
  private static final byte[] SERIALIZED_UN_1000L =
      toBytes(api().withTimeConversions(), UN_LONG, 1000L);

  public static <T> T fromBytes(
      GenericData model, Schema writerSchema, Schema readerSchema, byte[] serialized) {
    if (AvroVersion.avro_1_12.orAfter("TODO: This is a bug and shouldn't be necessary"))
      model.setFastReaderEnabled(false);
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().directBinaryDecoder(bais, null);
      DatumReader<T> r = new GenericDatumReader<>(writerSchema, readerSchema, model);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  @Test
  public void testWidenIntToDate() {
    assertThat(
        fromBytes(api().withTimeConversions(), INT, DATE, SERIALIZED_1000),
        hasToString("1972-09-27"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_INT, DATE, SERIALIZED_U_1000),
        hasToString("1972-09-27"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_INT, DATE, SERIALIZED_UN_1000),
        hasToString("1972-09-27"));

    assertThat(
        fromBytes(api().withTimeConversions(), INT, U_DATE, SERIALIZED_1000),
        hasToString("1972-09-27"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_INT, U_DATE, SERIALIZED_U_1000),
        hasToString("1972-09-27"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_INT, U_DATE, SERIALIZED_UN_1000),
        hasToString("1972-09-27"));

    assertThat(
        fromBytes(api().withTimeConversions(), INT, UN_DATE, SERIALIZED_1000),
        hasToString("1972-09-27"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_INT, UN_DATE, SERIALIZED_U_1000),
        hasToString("1972-09-27"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_INT, UN_DATE, SERIALIZED_UN_1000),
        hasToString("1972-09-27"));
  }

  @Test
  public void testWidenIntToTimeMicros() {
    assertThat(
        fromBytes(api().withTimeConversions(), INT, TIMEMICROS, SERIALIZED_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_INT, TIMEMICROS, SERIALIZED_U_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_INT, TIMEMICROS, SERIALIZED_UN_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), LONG, TIMEMICROS, SERIALIZED_1000L),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_LONG, TIMEMICROS, SERIALIZED_U_1000L),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_LONG, TIMEMICROS, SERIALIZED_UN_1000L),
        hasToString("00:00:00.001"));

    assertThat(
        fromBytes(api().withTimeConversions(), INT, U_TIMEMICROS, SERIALIZED_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_INT, U_TIMEMICROS, SERIALIZED_U_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_INT, U_TIMEMICROS, SERIALIZED_UN_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), LONG, U_TIMEMICROS, SERIALIZED_1000L),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_LONG, U_TIMEMICROS, SERIALIZED_U_1000L),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_LONG, U_TIMEMICROS, SERIALIZED_UN_1000L),
        hasToString("00:00:00.001"));

    assertThat(
        fromBytes(api().withTimeConversions(), INT, UN_TIMEMICROS, SERIALIZED_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_INT, UN_TIMEMICROS, SERIALIZED_U_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_INT, UN_TIMEMICROS, SERIALIZED_UN_1000),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), LONG, UN_TIMEMICROS, SERIALIZED_1000L),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), U_LONG, UN_TIMEMICROS, SERIALIZED_U_1000L),
        hasToString("00:00:00.001"));
    assertThat(
        fromBytes(api().withTimeConversions(), UN_LONG, UN_TIMEMICROS, SERIALIZED_UN_1000L),
        hasToString("00:00:00.001"));
  }

  @Test
  public void testWidenUnionIntToUnionTimeMicros() {
    final Schema writeSchema = api().createUnion(Schema.create(Schema.Type.INT));
    final Schema readSchema =
        api()
            .createUnion(
                LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))); // LONG

    byte[] x = toBytes(api().withTimeConversions(), writeSchema, 1000);

    Object xy = fromBytes(api().withTimeConversions(), writeSchema, readSchema, x);
    assertThat(xy, hasToString("00:00:00.001"));
  }

  @Test
  public void testWidenUnionIntToUnionDate() {
    final Schema writeSchema = api().createUnion(Schema.create(Schema.Type.INT));
    final Schema readSchema =
        api().createUnion(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));

    byte[] x = toBytes(api().withTimeConversions(), writeSchema, 1000);

    Object xy = fromBytes(api().withTimeConversions(), writeSchema, readSchema, x);
    assertThat(xy, hasToString("1972-09-27"));
  }

  @Test
  public void testWidenIntToUnionDate() {
    final Schema writeSchema = Schema.create(Schema.Type.INT);
    final Schema readSchema =
        api().createUnion(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));

    byte[] x = toBytes(api().withTimeConversions(), writeSchema, 1000);

    Object xy = fromBytes(api().withTimeConversions(), writeSchema, readSchema, x);
    assertThat(xy, hasToString("1972-09-27"));
  }

  @Test
  public void testWidenUnionIntToDate() {
    final Schema writeSchema = api().createUnion(Schema.create(Schema.Type.INT));
    final Schema readSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));

    byte[] x = toBytes(api().withTimeConversions(), writeSchema, 1000);

    Object xy = fromBytes(api().withTimeConversions(), writeSchema, readSchema, x);
    assertThat(xy, hasToString("1972-09-27"));
  }

  @Test
  public void testWidenLongToTimeMicros() {
    final Schema writeSchema = Schema.create(Schema.Type.LONG);
    final Schema readSchema =
        LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)); // LONG

    byte[] x = toBytes(api().withTimeConversions(), writeSchema, 1000L);

    Object xy = fromBytes(api().withTimeConversions(), writeSchema, readSchema, x);
    assertThat(xy, hasToString("00:00:00.001"));
  }

  @Test
  public void testWidenUnionLongToUnionTimeMicros() {
    final Schema writeSchema = api().createUnion(Schema.create(Schema.Type.LONG));
    final Schema readSchema =
        api()
            .createUnion(
                LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))); // LONG

    byte[] x = toBytes(api().withTimeConversions(), writeSchema, 1000L);

    Object xy = fromBytes(api().withTimeConversions(), writeSchema, readSchema, x);
    assertThat(xy, hasToString("00:00:00.001"));
  }

  @Test
  @Disabled("AVRO-4215")
  public void testWidenUnionIntToDateJira() {
    GenericData model = SpecificData.get().setFastReaderEnabled(true);

    // ["int"]
    final Schema writeSchema = SchemaBuilder.unionOf().intType().endUnion();
    final byte[] serialized;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<Integer> w = new GenericDatumWriter<>(writeSchema, model);
      w.write(1000, encoder);
      encoder.flush();
      serialized = baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }

    // {"type":"int","logicalType":"date"}
    final Schema readSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    final Object deserialized;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().directBinaryDecoder(bais, null);
      DatumReader<?> r = new GenericDatumReader<>(writeSchema, readSchema, model);
      deserialized = r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }

    assertThat(deserialized, hasToString(containsString("1972-09-27")));
  }
}
