package com.skraba.avro.enchiridion.testkit;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.CanIgnoreReturnValue;

/** Add custom assertions on Avro classes. */
@CanIgnoreReturnValue
public class AvroAssertions extends Assertions {

  public static AbstractSchemaAssert<?, ? extends Schema> assertThat(Schema actual) {
    return new SchemaAssert(actual);
  }

  public static AbstractGenericContainerAssert<?, ? extends GenericContainer> assertThat(
      GenericContainer actual) {
    return new GenericContainerAssert(actual);
  }

  public static AbstractIndexedRecordAssert<?, ? extends IndexedRecord> assertThat(
      IndexedRecord actual) {
    return new IndexedRecordAssert(actual);
  }

  public static AbstractGenericRecordAssert<?, ? extends GenericRecord> assertThat(
      GenericRecord actual) {
    return new GenericRecordAssert(actual);
  }

  public static CompatibilityPairAssert assertThat(
      SchemaCompatibility.SchemaPairCompatibility actual) {
    return new CompatibilityPairAssert(actual);
  }

  public static SchemaCompatibilityResultAssert assertThat(
      SchemaCompatibility.SchemaCompatibilityResult actual) {
    return new SchemaCompatibilityResultAssert(actual);
  }

  public abstract static class AbstractSchemaAssert<
          SELF extends AbstractSchemaAssert<SELF, ACTUAL>, ACTUAL extends Schema>
      extends AbstractObjectAssert<SELF, ACTUAL> {

    protected AbstractSchemaAssert(ACTUAL actual, Class<?> selfType) {
      super(actual, selfType);
    }

    public CompatibilityPairAssert compatibilityWith(Schema reader) {
      return assertThat(SchemaCompatibility.checkReaderWriterCompatibility(reader, actual));
    }

    public NamedSchemaAssert isNamed() {
      isNotNull();
      if (actual.getType() != Schema.Type.RECORD
          && actual.getType() != Schema.Type.ENUM
          && actual.getType() != Schema.Type.FIXED) {
        failWithMessage("Expected to have a named schema but was %s", actual.getType());
      }
      return new NamedSchemaAssert(actual);
    }

    public RecordSchemaAssert isRecord() {
      isNotNull();
      if (actual.getType() != Schema.Type.RECORD) {
        failWithMessage("Expected to have RECORD but was %s", actual.getType());
      }
      return new RecordSchemaAssert(actual);
    }
  }

  public static class SchemaAssert extends AbstractSchemaAssert<SchemaAssert, Schema> {

    public SchemaAssert(Schema actual) {
      super(actual, SchemaAssert.class);
    }
  }

  public abstract static class AbstractNamedSchemaAssert<
          SELF extends AbstractNamedSchemaAssert<SELF, ACTUAL>, ACTUAL extends Schema>
      extends AbstractSchemaAssert<SELF, ACTUAL> {

    protected AbstractNamedSchemaAssert(ACTUAL actual, Class<?> selfType) {
      super(actual, selfType);
    }

    public SELF hasName(String name) {
      isNotNull();
      assertThat(actual.getName()).as("Checking the name", actual.getType()).isEqualTo(name);
      return myself;
    }

    public SELF hasNamespace(String namespace) {
      isNotNull();
      assertThat(actual.getNamespace())
          .as("Checking the namespace", actual.getType())
          .isEqualTo(namespace);
      return myself;
    }

    public SELF hasFullName(String fullName) {
      isNotNull();
      assertThat(actual.getFullName())
          .as("Checking the full name", actual.getType())
          .isEqualTo(fullName);
      return myself;
    }
  }

  public static class NamedSchemaAssert
      extends AbstractNamedSchemaAssert<NamedSchemaAssert, Schema> {

    public NamedSchemaAssert(Schema actual) {
      super(actual, NamedSchemaAssert.class);
    }
  }

  public abstract static class AbstractRecordSchemaAssert<
          SELF extends AbstractRecordSchemaAssert<SELF, ACTUAL>, ACTUAL extends Schema>
      extends AbstractNamedSchemaAssert<SELF, ACTUAL> {

    protected AbstractRecordSchemaAssert(ACTUAL actual, Class<?> selfType) {
      super(actual, selfType);
    }

    public SELF hasFieldAt(int index) {
      isNotNull();
      assertThat(index).withFailMessage("The field index %s is invalid", index).isNotNegative();
      assertThat(actual.getFields())
          .withFailMessage(
              "The record %s has %s field(s): %s is out of range",
              actual.getFullName(), actual.getFields().size(), index)
          .hasSizeGreaterThan(index);
      return myself;
    }

    public SELF hasFieldNamed(String fieldName) {
      isNotNull();
      assertThat(actual.getField(fieldName))
          .withFailMessage("The %s field doesn't exist in %s", fieldName, actual.getFullName())
          .isNotNull();
      return myself;
    }
  }

  public static class RecordSchemaAssert
      extends AbstractRecordSchemaAssert<RecordSchemaAssert, Schema> {

    public RecordSchemaAssert(Schema actual) {
      super(actual, RecordSchemaAssert.class);
    }
  }

  public abstract static class AbstractGenericContainerAssert<
          SELF extends AbstractGenericContainerAssert<SELF, ACTUAL>,
          ACTUAL extends GenericContainer>
      extends AbstractObjectAssert<SELF, ACTUAL> {

    protected AbstractGenericContainerAssert(ACTUAL actual, Class<?> selfType) {
      super(actual, selfType);
    }

    public SELF hasSchema(Schema schema) {
      isNotNull();
      if (!actual.getSchema().equals(schema)) {
        failWithMessage("Expected to have Schema %s but was %s", schema, actual.getSchema());
      }
      return myself;
    }
  }

  public static class GenericContainerAssert
      extends AbstractGenericContainerAssert<GenericContainerAssert, GenericContainer> {

    public GenericContainerAssert(GenericContainer actual) {
      super(actual, GenericContainerAssert.class);
    }
  }

  public static class AbstractIndexedRecordAssert<
          SELF extends AbstractIndexedRecordAssert<SELF, ACTUAL>, ACTUAL extends IndexedRecord>
      extends AbstractGenericContainerAssert<SELF, ACTUAL> {

    protected AbstractIndexedRecordAssert(ACTUAL actual, Class<?> selfType) {
      super(actual, selfType);
    }

    public SELF hasField(int index) {
      isNotNull();
      assertThat(actual.getSchema()).isRecord().hasFieldAt(index);
      return myself;
    }

    public SELF hasFieldEqualTo(int index, String value) {
      hasField(index);
      assertThat(actual.get(index)).as("Field index %s", index).hasToString(value);
      return myself;
    }

    public SELF hasFieldEqualTo(int index, Object value) {
      hasField(index);
      assertThat(actual.get(index)).as("Field index %s", index).isEqualTo(value);
      return myself;
    }
  }

  public static class IndexedRecordAssert
      extends AbstractIndexedRecordAssert<IndexedRecordAssert, IndexedRecord> {

    public IndexedRecordAssert(IndexedRecord actual) {
      super(actual, IndexedRecordAssert.class);
    }
  }

  public static class AbstractGenericRecordAssert<
          SELF extends AbstractGenericRecordAssert<SELF, ACTUAL>, ACTUAL extends GenericRecord>
      extends AbstractIndexedRecordAssert<SELF, ACTUAL> {

    protected AbstractGenericRecordAssert(ACTUAL actual, Class<?> selfType) {
      super(actual, selfType);
    }

    public SELF hasField(String key) {
      isNotNull();
      assertThat(actual.getSchema()).isRecord().hasFieldNamed(key);
      return myself;
    }

    public SELF hasFieldEqualTo(String key, String value) {
      hasField(key);
      assertThat(actual.get(key)).as("Field %s", key).hasToString(value);
      return myself;
    }

    public SELF hasFieldEqualTo(String key, Object value) {
      hasField(key);
      assertThat(actual.get(key)).as("Field %s", key).isEqualTo(value);
      return myself;
    }
  }

  public static class GenericRecordAssert
      extends AbstractGenericRecordAssert<GenericRecordAssert, GenericRecord> {

    public GenericRecordAssert(GenericRecord actual) {
      super(actual, GenericRecordAssert.class);
    }
  }

  public static class CompatibilityPairAssert
      extends AbstractObjectAssert<
          CompatibilityPairAssert, SchemaCompatibility.SchemaPairCompatibility> {

    protected CompatibilityPairAssert(SchemaCompatibility.SchemaPairCompatibility actual) {
      super(actual, CompatibilityPairAssert.class);
    }

    public void isOK() {
      assertThat(actual.getType())
          .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
      if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
        assertThat(actual.getResult()).isCompatible();
      }
    }

    public void isNotOK(String... reasons) {
      assertThat(actual.getType())
          .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE);
      if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
        assertThat(actual.getResult()).isIncompatible();
        assertThat(actual.getResult().getIncompatibilities()).hasSameSizeAs(reasons);
        for (int i = 0; i < reasons.length; i++) {
          assertThat(actual.getResult()).hasIncompatibilityType(i, reasons[i]);
        }
      }
    }
  }

  public static class SchemaCompatibilityResultAssert
      extends AbstractObjectAssert<
          SchemaCompatibilityResultAssert, SchemaCompatibility.SchemaCompatibilityResult> {

    protected SchemaCompatibilityResultAssert(
        SchemaCompatibility.SchemaCompatibilityResult actual) {
      super(actual, SchemaCompatibilityResultAssert.class);
    }

    public void isCompatible() {
      assertThat(actual.getCompatibility())
          .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
      assertThat(actual).isEqualTo(SchemaCompatibility.SchemaCompatibilityResult.compatible());
    }

    public void isIncompatible() {
      assertThat(actual.getCompatibility())
          .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE);
    }

    public void hasIncompatibilityType(int i, String reason) {
      assertThat(actual.getIncompatibilities().get(i).getType())
          .as("Reason %s")
          .hasToString(reason);
    }
  }
}
