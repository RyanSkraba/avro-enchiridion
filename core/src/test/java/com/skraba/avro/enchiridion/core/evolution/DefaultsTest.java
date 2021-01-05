package com.skraba.avro.enchiridion.core.evolution;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static com.skraba.avro.enchiridion.core.AvroUtil.jsonify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.core.AvroVersion;
import com.skraba.avro.enchiridion.resources.AvroTestResources;
import com.skraba.avro.enchiridion.resources.NumericValues;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Unit tests for default values in schemas. */
public class DefaultsTest {

  public static boolean isFinite(Object defaultVal) {
    return defaultVal instanceof Number && Double.isFinite(((Number) defaultVal).doubleValue());
  }

  @Test
  void testParsingValidDefault() throws Throwable {
    String schema =
        AvroTestResources.RecordOneFieldWithDefault(
            "ValidDefault", "id", jsonify("long"), jsonify(-1));
    Schema s = new Schema.Parser().parse(schema);
    assertThat(s, notNullValue());
  }

  @Test
  void testParsingValidNullableDefault() throws Throwable {
    String schema =
        AvroTestResources.RecordOneFieldWithDefault(
            "ValidNullableDefault", "id", "[\"long\", \"null\"]", jsonify(-1));
    Schema s = new Schema.Parser().parse(schema);
    assertThat(s, notNullValue());
  }

  @Test
  void testParsingValidNullDefault() throws Throwable {
    String schema =
        AvroTestResources.RecordOneFieldWithDefault(
            "ValidNullDefault", "id", "[\"null\", \"long\"]", jsonify(null));
    Schema s = new Schema.Parser().parse(schema);
    assertThat(s, notNullValue());
  }

  /** See https://issues.apache.org/jira/browse/AVRO-2817 */
  @Test
  void testParsingInvalidNullDefault() throws Throwable {
    String schema =
        AvroTestResources.RecordOneFieldWithDefault(
            "InvalidNullDefault", "id", "[\"long\", \"null\"]", jsonify(null));

    if (AvroVersion.avro_1_9.before()) {
      // Previous to 1.9, this would have succeeded.
      Schema s = new Schema.Parser().parse(schema);
      assertThat(s, notNullValue());
    } else {
      // After 1.9, this became an exception.
      AvroTypeException ex =
          assertThrows(
              AvroTypeException.class,
              () -> {
                Schema s = new Schema.Parser().parse(schema);
              });
      assertThat(
          ex.getMessage(), is("Invalid default for field id: null not a [\"long\",\"null\"]"));
    }
  }

  @ParameterizedTest
  @MethodSource("getNumberDefaults")
  void testDoubleWithNumbers(String tag, Object defaultVal) throws Throwable {

    // Create the schema from the Schema API.
    Supplier<Schema> viaSchemaApi =
        () ->
            api()
                .createRecord(
                    "FieldDoubleDefault" + tag,
                    null,
                    null,
                    false,
                    Collections.singletonList(
                        api()
                            .createField(
                                "a", Schema.create(Schema.Type.DOUBLE), null, defaultVal)));

    // Create the schema from the SchemaBuilder API.
    Supplier<Schema> viaSchemaBuilder =
        () ->
            SchemaBuilder.record("FieldDoubleDefault" + tag)
                .fields()
                .name("a")
                .type(Schema.create(Schema.Type.DOUBLE))
                .withDefault(defaultVal)
                .endRecord();

    // Create the schema from a JSON String.
    Supplier<Schema> viaSchemaParser =
        () ->
            api()
                .parse(
                    AvroTestResources.RecordOneFieldWithDefault(
                        "FieldDoubleDefault" + tag, "a", jsonify("double"), jsonify(defaultVal)));

    if (isFinite(defaultVal)) {
      Schema schemaBuilder = viaSchemaBuilder.get();
      Schema schemaParser = viaSchemaBuilder.get();
      assertThat(schemaParser.toString(), is(schemaBuilder.toString()));

      if (AvroVersion.avro_1_8.orAfter()
          && (defaultVal instanceof Short || defaultVal instanceof Byte)) {
        // TODO: This shouldn't throw exception.
        Exception exception1 = assertThrows(AvroRuntimeException.class, viaSchemaApi::get);
      } else {
        Schema schemaApi = viaSchemaApi.get();
        if (!(defaultVal instanceof Float) || AvroVersion.avro_1_9.orAfter())
          // Before Avro 1.9, Float values changed precision between these two.
          assertThat(schemaApi.toString(), is(schemaBuilder.toString()));
      }

    } else {
      // TODO: How are we going to deal with non-standard numbers?
      Schema schemaApi = viaSchemaApi.get();
      Exception exception1 = assertThrows(SchemaParseException.class, viaSchemaParser::get);
      if (AvroVersion.avro_1_9.orAfter()) {
        Exception exception2 = assertThrows(AvroTypeException.class, viaSchemaBuilder::get);
      } else {
        Schema schemaBuilder = viaSchemaBuilder.get();
      }
    }
  }

  private static Stream<Arguments> getNumberDefaults() {
    return NumericValues.AllJava().entrySet().stream()
        .map(kv -> Arguments.of(kv.getKey(), kv.getValue()));
  }
}
