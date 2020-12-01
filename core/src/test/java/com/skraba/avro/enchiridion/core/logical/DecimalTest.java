package com.skraba.avro.enchiridion.core.logical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.skraba.avro.enchiridion.core.file.AvroFileTest;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  @Test
  public void testAddingConversionToGenericDataForFileWrite(@TempDir Path tmpDir)
      throws IOException {

    // Create a record with one field that is a nullable decimal logical type.
    GenericRecord r =
        new GenericData.Record(
            SchemaBuilder.record(
                    "com.skraba.avro.enchiridion.core.logical.testAddingConversionToGenericData")
                .fields()
                .name("value")
                .type()
                .unionOf()
                .nullType()
                .and()
                .type(bytesSchema)
                .endUnion()
                .noDefault()
                .endRecord());
    r.put("value", BigDecimal.valueOf(10.0));

    // This fails because the GenericData model does not know the logical type by default.
    assertThrows(
        org.apache.avro.file.DataFileWriter.AppendWriteException.class,
        () -> {
          DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(r.getSchema());
          try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            File testFile = tmpDir.resolve("should_fail.avro").toFile();
            dataFileWriter.create(r.getSchema(), testFile);
            dataFileWriter.append(r);
          }
        });

    // So create and use a model that knows about this logical type conversion.
    GenericData model = new GenericData();
    model.addLogicalTypeConversion(new Conversions.DecimalConversion());

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(r.getSchema(), model);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      File testFile = tmpDir.resolve("succeeds.avro").toFile();
      dataFileWriter.create(r.getSchema(), testFile);
      dataFileWriter.append(r);
    }

    // Check that the record did succeed.
    GenericData.Record read =
        AvroFileTest.fromFile(tmpDir.resolve("succeeds.avro").toFile(), model);
    assertThat(read.get(0), is(new BigDecimal("10.00")));
  }
}
