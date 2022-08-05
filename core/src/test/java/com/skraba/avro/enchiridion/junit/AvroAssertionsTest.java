package com.skraba.avro.enchiridion.junit;

import static com.skraba.avro.enchiridion.junit.AvroAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

/** Test that the assertions and error messages on {@link AvroAssertions} are meaningful. */
class AvroAssertionsTest {

  Schema RECORD_SCHEMA =
      SchemaBuilder.record("ns.Record")
          .fields()
          .requiredLong("id")
          .requiredString("name")
          .endRecord();

  Schema ENUM_SCHEMA = SchemaBuilder.enumeration("ns.Enum").symbols("e1", "e2", "e3");

  Schema DOUBLE_SCHEMA = SchemaBuilder.builder().doubleType();

  /**
   * Helper method for constructing the expected error messages that have been "slightly" customised
   * from AssertJ. These are typically three lines, with the message, the expected and actual value.
   */
  public static String msg(String description, String expected, String actual) {
    return String.format("[%s] \nexpected: %s\n but was: %s", description, expected, actual);
  }

  /**
   * Helper method for constructing the expected error messages that have been "slightly" customised
   * from AssertJ. These are typically three lines, with the message, the expected and actual value.
   * The String values will have additional quotes applied.
   */
  public static String msgqq(String description, String expected, String actual) {
    return String.format(
        "[%s] \nexpected: \"%s\"\n but was: \"%s\"", description, expected, actual);
  }

  @Test
  public void testNamedSchema() {
    assertThat(RECORD_SCHEMA)
        .isNamed()
        .hasName("Record")
        .hasNamespace("ns")
        .hasFullName("ns.Record");

    assertThat(ENUM_SCHEMA).isNamed().hasName("Enum").hasNamespace("ns").hasFullName("ns.Enum");

    // Error when trying to call on a non-named record
    assertThatThrownBy(() -> assertThat(DOUBLE_SCHEMA).isNamed())
        .hasMessage("Expected to have a named schema but was DOUBLE");

    // Other assertion errors
    assertThatThrownBy(() -> assertThat(ENUM_SCHEMA).isNamed().hasName("Nope"))
        .hasMessage(msgqq("Checking the name", "Nope", "Enum"));
    assertThatThrownBy(() -> assertThat(ENUM_SCHEMA).isNamed().hasNamespace("Nope"))
        .hasMessage(msgqq("Checking the namespace", "Nope", "ns"));
    assertThatThrownBy(() -> assertThat(ENUM_SCHEMA).isNamed().hasFullName("Nope"))
        .hasMessage(msgqq("Checking the full name", "Nope", "ns.Enum"));
  }

  @Test
  public void testRecordSchema() {
    assertThat(RECORD_SCHEMA)
        .isRecord()
        .hasName("Record")
        .hasNamespace("ns")
        .hasFullName("ns.Record")
        .hasFieldAt(0)
        .hasFieldAt(1)
        .hasFieldNamed("id")
        .hasFieldNamed("name");

    // Error when trying to call on a non-record
    assertThatThrownBy(() -> assertThat(ENUM_SCHEMA).isRecord())
        .hasMessage("Expected to have RECORD but was ENUM");

    // Other assertion errors
    assertThatThrownBy(() -> assertThat(RECORD_SCHEMA).isRecord().hasName("Nope"))
        .hasMessage(msgqq("Checking the name", "Nope", "Record"));
    assertThatThrownBy(() -> assertThat(RECORD_SCHEMA).isRecord().hasNamespace("Nope"))
        .hasMessage(msgqq("Checking the namespace", "Nope", "ns"));
    assertThatThrownBy(() -> assertThat(RECORD_SCHEMA).isRecord().hasFullName("Nope"))
        .hasMessage(msgqq("Checking the full name", "Nope", "ns.Record"));
    assertThatThrownBy(() -> assertThat(RECORD_SCHEMA).isRecord().hasFieldAt(-1))
        .hasMessage("The field index -1 is invalid");
    assertThatThrownBy(() -> assertThat(RECORD_SCHEMA).isRecord().hasFieldAt(2))
        .hasMessage("The record ns.Record has 2 field(s): 2 is out of range");
    assertThatThrownBy(() -> assertThat(RECORD_SCHEMA).isRecord().hasFieldNamed("nope"))
        .hasMessage("The nope field doesn't exist in ns.Record");
  }

  @Test
  public void testGenericRecord() {
    GenericRecord r =
        new GenericRecordBuilder(RECORD_SCHEMA).set("id", 1L).set("name", "one").build();

    assertThat(r)
        .hasSchema(RECORD_SCHEMA)
        .hasFieldEqualTo("id", 1L)
        .hasFieldEqualTo("name", "one")
        .hasFieldEqualTo(0, 1L)
        .hasFieldEqualTo(1, "one");

    // Other assertion errors
    assertThatThrownBy(() -> assertThat(r).hasSchema(DOUBLE_SCHEMA))
        .hasMessage("Expected to have Schema %s but was %s", DOUBLE_SCHEMA, RECORD_SCHEMA);
    assertThatThrownBy(() -> assertThat(r).hasFieldEqualTo(0, 10L))
        .hasMessage(msg("Field index 0", "10L", "1L"));
  }
}
