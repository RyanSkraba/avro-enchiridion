package com.skraba.avro.enchiridion.core;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.skraba.avro.enchiridion.resources.AvroTestResources;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

/** Unit tests for working with the {@link GenericData} class. */
public class GenericDataTest {

  @Test
  public void testValidateRenamedField() {
    Schema simpleV1 = api().parse(AvroTestResources.SimpleRecord());
    Schema simpleV2 =
        api().parse(AvroTestResources.SimpleRecord().replaceFirst("\"id\"", "\"uid\""));

    GenericRecord recordV1 =
        new GenericRecordBuilder(simpleV1).set("id", 1L).set("name", "One").build();

    // The validate method succeeds because it does not validate the field name just the position...
    // So the test fails.
    assertTrue(GenericData.get().validate(simpleV1, recordV1));
    assertTrue(GenericData.get().validate(simpleV2, recordV1));

    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(simpleV1, simpleV2);
    assertThat(
        compatibility.getType(), is(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE));
  }
}
