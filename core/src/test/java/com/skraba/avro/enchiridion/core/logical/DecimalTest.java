package com.skraba.avro.enchiridion.core.logical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

/** Unit tests for the Avro decimal type. */
public class DecimalTest {

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

  @Test
  public void testValidateDecimalSchemas() {
    // You can validate schemas that are already logical types.
    LogicalTypes.decimal(5, 2).validate(bytesSchema);
    LogicalTypes.decimal(5, 2).validate(fixedSchema);
    LogicalTypes.decimal(5, 2).validate(fixedSchemaBig);

    // Or not
    LogicalTypes.decimal(5, 2).validate(Schema.create(Schema.Type.BYTES));
    LogicalTypes.decimal(5, 2).validate(Schema.createFixed("fixed", null, null, 3));
    LogicalTypes.decimal(5, 2).validate(Schema.createFixed("fixed", null, null, 10));

    try {
      LogicalTypes.decimal(5, 2).validate(Schema.createFixed("fixed", null, null, 1));
      fail("Not a valid base type for the given precision.");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("fixed(1) cannot store 5 digits (max 2)"));
    }
  }
}
