package com.skraba.avro.enchiridion.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.simple.BuilderWithRequiredAndOptional;
import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.AvroRuntimeException;
import org.junit.jupiter.api.Test;

public class BuilderWithRequiredAndOptionalTest {

  @Test
  public void testBuilderWithUnsetFields() {
    // All of the no-default fields must be set, but the others can be left unset.
    BuilderWithRequiredAndOptional r =
        BuilderWithRequiredAndOptional.newBuilder()
            .setRequiredNoDefault("r")
            .setOptionalNoDefault("o")
            .setNullableNoDefault("n")
            .build();
    assertThat(r.get("required").toString(), is(""));
    assertThat(r.get("optional").toString(), is(""));
    assertThat(r.get("nullable"), nullValue());
    assertThat(r.get("required_no_default"), is("r"));
    assertThat(r.get("optional_no_default"), is("o"));
    assertThat(r.get("nullable_no_default"), is("n"));

    // If any of the three are missing, an exception occurs
    AvroMissingFieldException ex =
        assertThrows(
            AvroMissingFieldException.class,
            () -> BuilderWithRequiredAndOptional.newBuilder().build());
    assertThat(
        ex.getMessage(),
        is("Field required_no_default type:STRING pos:3 not set and has no default value"));

    ex =
        assertThrows(
            AvroMissingFieldException.class,
            () ->
                BuilderWithRequiredAndOptional.newBuilder()
                    .setOptionalNoDefault("o")
                    .setNullableNoDefault("n")
                    .build());
    assertThat(
        ex.getMessage(),
        is("Field required_no_default type:STRING pos:3 not set and has no default value"));

    ex =
        assertThrows(
            AvroMissingFieldException.class,
            () ->
                BuilderWithRequiredAndOptional.newBuilder()
                    .setRequiredNoDefault("r")
                    .setNullableNoDefault("n")
                    .build());
    assertThat(
        ex.getMessage(),
        is("Field optional_no_default type:UNION pos:4 not set and has no default value"));

    ex =
        assertThrows(
            AvroMissingFieldException.class,
            () ->
                BuilderWithRequiredAndOptional.newBuilder()
                    .setRequiredNoDefault("r")
                    .setOptionalNoDefault("o")
                    .build());
    assertThat(
        ex.getMessage(),
        is("Field nullable_no_default type:UNION pos:5 not set and has no default value"));
  }

  @Test
  public void testBuilderWithNullFields() {
    // Throw an exception when setting a non-nullable field to null.
    AvroRuntimeException ex =
        assertThrows(
            AvroRuntimeException.class,
            () -> BuilderWithRequiredAndOptional.newBuilder().setRequiredNoDefault(null));
    assertThat(
        ex.getMessage(),
        is("Field required_no_default type:STRING pos:3 does not accept null values"));

    /* TODO(AVRO-3091): This should ALSO throw an exception.
    ex = assertThrows(
            AvroRuntimeException.class,
            () -> BuilderWithRequiredAndOptional.newBuilder().setRequired(null));
    assertThat(
        e.getMessage(),
        is("Field required_no_default type:STRING pos:3 does not accept null values"));
     */
  }
}
