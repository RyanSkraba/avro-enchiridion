package com.skraba.avro.enchiridion.core.schema;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.resources.AvroTestResources;
import java.util.Arrays;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.junit.jupiter.api.Test;

/** Unit tests for custom properties in a schema. */
public class UserPropertiesTest {

  @Test
  public void testAddAPropertyToAPrimitive() {
    Schema schema = Schema.create(Schema.Type.NULL);
    schema.addProp("user-property", 100);

    assertThat(schema.getProp("user-property"), nullValue());
    assertThat(schema.getObjectProp("user-property"), is(100));
  }

  @Test
  public void testAddAReservedPropertyToPrimitiveSize() {
    assertThrows(
        AvroRuntimeException.class, () -> Schema.create(Schema.Type.NULL).addProp("size", 100));
  }

  @Test
  public void testAddAReservedPropertyToPrimitiveDefault() {
    // TODO: This should probably throw an exception?
    Schema.create(Schema.Type.NULL).addProp("default", 100);
  }

  @Test
  public void testAddAReservedPropertyToSchema() {
    assertThrows(
        AvroRuntimeException.class,
        () -> api().parse(AvroTestResources.SimpleRecord()).addProp("size", 100));
  }

  @Test
  public void testAddAReservedPropertyToEnum() {
    assertThrows(
        AvroRuntimeException.class,
        () ->
            Schema.createEnum("a", null, null, Arrays.asList("a", "b", "c")).addProp("size", 100));
  }

  @Test
  public void testAddAReservedPropertyToField() {
    Schema schema = api().parse(AvroTestResources.SimpleRecord());
    // TODO: This should probably throw an exception?
    schema.getFields().get(0).addProp("size", 100);
  }

  @Test
  public void testIgnoreReservedPropertyWhenParsing() {
    Schema record = api().parse(AvroTestResources.Avro2299CanonicalMisplacedSize());

    // Out-of-place reserved attribute is ignored.
    assertThat(record.getProp("user-property"), is("There is no size attribute in a record."));
    assertThat(record.getProp("size"), nullValue());

    // Out-of-place reserved attribute is ignored.
    Schema.Field a1Field = record.getFields().get(0);
    assertThat(
        a1Field.getObjectProp("user-property"), is("There is no size attribute in a field."));
    // TODO: This should be invalid.
    assertThat(a1Field.getObjectProp("size"), is(200));

    Schema a1 = a1Field.schema();
    assertThat(a1.getObjectProp("user-property"), is("There is no size attribute in an enum."));
    assertThat(a1.getObjectProp("size"), nullValue());

    Schema a2 = record.getFields().get(1).schema();
    assertThat(a2.getObjectProp("user-property"), is("There is a size attribute in a fixed."));
    assertThat(a2.getObjectProp("size"), nullValue());
    assertThat(a2.getFixedSize(), is(123));

    Schema pcRecord = api().parse(SchemaNormalization.toParsingForm(record));
    assertThat(pcRecord.getObjectProps().entrySet(), hasSize(0));
    assertThat(pcRecord.getFields().get(0).getObjectProps().entrySet(), hasSize(0));
    assertThat(pcRecord.getFields().get(0).schema().getObjectProps().entrySet(), hasSize(0));

    long fingerprint64 = SchemaNormalization.parsingFingerprint64(record);
    long parsedFingerprint64 = SchemaNormalization.parsingFingerprint64(pcRecord);
    assertThat(fingerprint64, is(parsedFingerprint64));
  }
}
