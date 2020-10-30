package com.skraba.avro.enchiridion.core.schema;

import com.skraba.avro.enchiridion.core.AvroVersion;

import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The {@link SchemaBuilder} and {@GenericBuilder} API have some tricks to them! */
public class BuildersTest {

  /** Demonstrates the difference between required, nullable and optional. */
  public static final Schema NULLABILITY =
      SchemaBuilder.record("Nullability")
          .fields()
          .requiredString("required")
          .optionalString("optional")
          .nullableString("nullable", "dflt")
          .endRecord();

  /**
   * Test differences between required, optional and nullable when using the {@link SchemaBuilder}.
   *
   * <p><b>Required</b> means that the datum must be present and non-null. There is no default.
   *
   * <p><b>Optional</b> and <b>nullable</b> permit null values (i.e. the datum type is a UNION with
   * NULL as an option). The only difference is whether NULL is first or last in the UNION.
   *
   * <p>A <b>default</b> value must correspond to the first type in the UNION, so an optional field
   * has a default of null, and a nullable field must have a specified, non-null default.
   *
   * <p>Note this usage of "default" is slightly semantically different than what default values
   * normally mean in Avro. Default values are used to fill in information when a field is added
   * during schema evolution, so required fields can also have default values.
   *
   * <p>The {@link GenericRecordBuilder}, however, <i>also</i> uses the default value if the user
   * did not explicitly specify the value while constructing the record. This is not the case when
   * building a {@link org.apache.avro.generic.GenericData.Record} for example.
   */
  @Test
  public void testRequiredNullableAndOptionalBasic() {

    // The required field is a plain STRING
    assertThat(NULLABILITY.getField("required").schema().toString(), is("\"string\""));
    assertThat(NULLABILITY.getField("optional").schema().toString(), is("[\"null\",\"string\"]"));
    assertThat(NULLABILITY.getField("nullable").schema().toString(), is("[\"string\",\"null\"]"));

    // Default values are available in optional and nullable fields.
    if (AvroVersion.avro_1_8.orAfter()) {
      assertThat(NULLABILITY.getField("required").defaultVal(), nullValue());
      assertThat(NULLABILITY.getField("optional").defaultVal(), is(JsonProperties.NULL_VALUE));
      assertThat(NULLABILITY.getField("nullable").defaultVal(), is("dflt"));
    }

    if (AvroVersion.avro_1_9.orAfter()) {
      // hasDefaultValue was introduced in Avro 1.9
      assertThat(NULLABILITY.getField("required").hasDefaultValue(), is(false));
      assertThat(NULLABILITY.getField("optional").hasDefaultValue(), is(true));
      assertThat(NULLABILITY.getField("nullable").hasDefaultValue(), is(true));
    }

    // GenericRecordBuilder automatically fills in a field with the default.
    GenericRecord r = new GenericRecordBuilder(NULLABILITY).set("required", "one").build();
    assertThat(r.get("required"), is("one"));
    assertThat(r.get("optional"), nullValue());
    assertThat(r.get("nullable"), is(new Utf8("dflt")));

    // An exception is thrown if you try to build with a missing, required field.
    AvroRuntimeException ex =
        assertThrows(
            AvroRuntimeException.class, () -> new GenericRecordBuilder(NULLABILITY).build());
    assertThat(
        ex.getMessage(), is("Field required type:STRING pos:0 not set and has no default value"));
    if (AvroVersion.avro_1_9.orAfter()) {
      // AvroMissingFieldException was added in Avro 1.9
      assertThat(ex, instanceOf(AvroMissingFieldException.class));
    }

    // Using the GenericData.Record directly, you can create the record (but it can't be serialized
    // correctly yet, it's not valid until the required field is filled).  Note that the default
    // value of the last field is *unrelated* to whether the record is "valid" yet.
    GenericRecord r2 = new GenericData.Record(NULLABILITY);
    assertThat(r2.get("required"), nullValue());
    assertThat(r2.get("optional"), nullValue());
    assertThat(r2.get("nullable"), nullValue());
  }
}
