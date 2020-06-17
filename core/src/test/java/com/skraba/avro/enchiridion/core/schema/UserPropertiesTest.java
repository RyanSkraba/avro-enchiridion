package com.skraba.avro.enchiridion.core.schema;

import com.skraba.avro.enchiridion.resources.AvroTestResources;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.junit.jupiter.api.Test;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for custom properties in a schema. */
public class UserPropertiesTest {

  @Test
  public void testAPropertyToAPrimitive() {
    Schema schema = Schema.create(Schema.Type.NULL);
    schema.addProp("user-property", 100);

    assertThat(schema.getProp("user-property"), nullValue());
    assertThat(schema.getObjectProp("user-property"), is(100));
  }

  @Test
  public void testTryToAddAReservedProperty() {
    assertThrows(
        AvroRuntimeException.class, () -> Schema.create(Schema.Type.NULL).addProp("size", 100));
  }

  @Test
  public void testIgnoreReservedPropertyWhenParsing() {
    Schema record = api().parse(AvroTestResources.Avro2299CanonicalMisplacedSize());

    // Out-of-place reserved attribute is ignored.
    assertThat(record.getProp("user-property"), is("There is no size attribute in a record."));
    assertThat(record.getProp("size"), nullValue());

    // Out-of-place reserved attribute is ignored.
    Schema.Field fixedField = record.getFields().get(0);
    assertThat(fixedField.getProp("user-property"), is("There is no size attribute in a field."));
    assertThat(fixedField.getProp("size"), nullValue());

    Schema fixed = fixedField.schema();
    assertThat(fixed.getProp("user-property"), is("There is a size attribute in a fixed."));
    assertThat(fixed.getProp("size"), nullValue());
    assertThat(fixed.getFixedSize(), is(123));

    Schema pcRecord = api().parse(SchemaNormalization.toParsingForm(record));
    assertThat(pcRecord.getObjectProps().entrySet(), hasSize(0));
    assertThat(pcRecord.getFields().get(0).getObjectProps().entrySet(), hasSize(0));
    assertThat(pcRecord.getFields().get(0).schema().getObjectProps().entrySet(), hasSize(0));

    long fingerprint64 = SchemaNormalization.parsingFingerprint64(record);
    long parsedFingerprint64 = SchemaNormalization.parsingFingerprint64(pcRecord);
    assertThat(fingerprint64, is(parsedFingerprint64));
  }
}
