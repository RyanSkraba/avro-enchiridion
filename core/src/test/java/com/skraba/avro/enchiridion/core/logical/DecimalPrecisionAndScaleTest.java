package com.skraba.avro.enchiridion.core.logical;

import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.roundTripBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.skraba.avro.enchiridion.testkit.AvroVersion;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

/** Unit tests for scale and precision in the Avro decimal type. */
public class DecimalPrecisionAndScaleTest {

  /** A generic model that know decimal conversions. */
  private static final GenericData model = new GenericData();

  static {
    // In order to use BigDecimal datum in a generic model, you have to explicitly add the
    // conversion.
    model.addLogicalTypeConversion(new Conversions.DecimalConversion());
  }

  /** The decimal logical type with precision 5 and scale 2 represented on top of bytes data. */
  private final Schema bytesSchema =
      LogicalTypes.decimal(5, 2).addToSchema(SchemaBuilder.builder().bytesType());
  /**
   * The decimal logical type with precision 5 and scale 2 represented on top of fixed byte data.
   */
  private final Schema fixedSchema =
      LogicalTypes.decimal(5, 2).addToSchema(SchemaBuilder.builder().fixed("fixed").size(3));
  /**
   * The decimal logical type with precision 5 and scale 2 represented on top of fixed byte data,
   * larger than necessary.
   */
  private final Schema fixedSchemaBig =
      LogicalTypes.decimal(5, 2).addToSchema(SchemaBuilder.builder().fixed("fixed").size(10));
  /**
   * An example decimal number with precision 5 and scale 2. This is a "good" datum that corresponds
   * to the schema.
   */
  private final String d52 = "123.45";
  /** Numbers that can "fit" into the schema by discarding the original scale and precision. */
  private final List<String> dFittable =
      Arrays.asList(
          d52,
          "123.450", // precision 6, scale 3
          "0123.45", // precision 6, scale 2
          "23.450", // precision 5, scale 3
          "0123.4", // precision 5, scale 1
          "3.450", // precision 4, scale 3
          "23.45", // precision 4, scale 2
          "123.4", // precision 4, scale 1
          "0123" // precision 4, scale 0
          );
  /** Numbers that won't fit into the schema because they overflow. */
  private final List<String> dOverflow =
      Arrays.asList(
          "9123.45", // precision 6, scale 2,
          "9123.4", // precision 5, scale 1
          "9123" // precision 4, scale 0
          );
  /** Numbers that don't fit into the schema because they lose decimal digits. */
  private final List<String> dApproximate =
      Arrays.asList(
          "123.456", // precision 6, scale 3,
          "23.456", // precision 5, scale 3
          "3.456" // precision 4, scale 3
          );
  /** All of the numbers for testing. */
  private final List<String> all =
      Stream.of(dFittable, dOverflow, dApproximate)
          .flatMap(Collection::stream)
          .collect(Collectors.toList());

  /** Test what happens when putting BigDecimal datum into a decimal logical type. */
  @Test
  public void testUsingOtherScalesAndPrecisionsWithBigDecimalDatum() {
    for (Schema schema : Arrays.asList(bytesSchema, fixedSchema, fixedSchemaBig)) {
      if (AvroVersion.avro_1_10.orAfter("BigDecimals only accepted if it 'fits'")) {
        // After 1.10.x, a BigDecimal datum will only be accepted into the column when it can "fit"
        // into the decimal schema without overflowing or losing decimal places.

        for (String s : dFittable) {
          BigDecimal bd = new BigDecimal(s);
          // We can't directly compare against the original since the scale might have been modified
          // during the round trip.
          assertThat(roundTripBytes(model, schema, bd).doubleValue(), is(bd.doubleValue()));
        }

        for (String s : dApproximate) {
          BigDecimal bd = new BigDecimal(s);
          AvroTypeException rte =
              assertThrows(
                  AvroTypeException.class,
                  () -> roundTripBytes(model, schema, bd),
                  "We should not allow values that will lose significant digits: " + bd);
          assertThat(
              rte.getMessage(),
              is("Cannot encode decimal with scale 3 as scale 2 without rounding"));
        }

        for (String s : dOverflow) {
          BigDecimal bd = new BigDecimal(s);
          AvroTypeException rte =
              assertThrows(
                  AvroTypeException.class,
                  () -> roundTripBytes(model, schema, bd),
                  "We should not allow values that will overflow: " + bd);
          assertThat(
              rte.getMessage(),
              containsString("Cannot encode decimal with precision 6 as max precision 5"));
        }

      } else {

        // Previous to 1.10, only the scale was taken into account.
        for (String s : all) {
          BigDecimal bd = new BigDecimal(s);
          try {
            roundTripBytes(model, schema, bd);
            if (bd.scale() != 2) fail("Only an exact match on scale should succeed");
          } catch (RuntimeException e) {
            if (bd.scale() == 2) fail("Exact matches on scale should succeed.", e);
          }
        }
      }
    }
  }
}
