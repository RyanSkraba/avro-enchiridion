package com.skraba.avro.enchiridion.core.evolution;

import com.skraba.avro.enchiridion.resources.NumericValues;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Unit tests for default values in schemas. */
public class DefaultsTest {

  public static boolean isFinite(Object defaultVal) {
    return defaultVal instanceof Number && Double.isFinite(((Number) defaultVal).doubleValue());
  }

  @ParameterizedTest
  @MethodSource("getNumberDefaults")
  void testDoubleWithNumbers(Object defaultVal) {
    // Currently only finite numbers can be used as default values.
    if (isFinite(defaultVal)) {
      Schema schema =
          SchemaBuilder.record("RecordWithDoubleDefault")
              .fields()
              .name("a")
              .type(Schema.create(Schema.Type.DOUBLE))
              .withDefault(defaultVal)
              .endRecord();
    }
  }

  private static Stream<?> getNumberDefaults() {
    return NumericValues.AllJava().values().stream();
  }
}
