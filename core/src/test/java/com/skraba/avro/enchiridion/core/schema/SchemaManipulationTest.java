package com.skraba.avro.enchiridion.core.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.core.AvroVersion;
import com.skraba.avro.enchiridion.junit.EnabledForAvroVersion;
import com.skraba.avro.enchiridion.resources.AvroTestResources;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

/** Changing a schema programmatically can be complicated in Avro. */
public class SchemaManipulationTest {

  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_8,
      reason = "Avro API requires Jackson classes to create fields.")
  public void testAddAFieldToARecord() {

    // We have an original record.
    Schema schema = AvroUtil.api().parse(AvroTestResources.SimpleRecord());
    List<Schema.Field> fields = schema.getFields();

    // This is the field we want to add into the record.
    Schema.Field newField =
        new Schema.Field("price", Schema.create(Schema.Type.DOUBLE), null, null);

    // You can't add a field directly to the array.
    assertThrows(
        IllegalStateException.class,
        () -> {
          fields.add(newField);
        });

    // You can't create a new mutable list with the existing fields, and add the new one.
    AvroRuntimeException e1 =
        assertThrows(
            AvroRuntimeException.class,
            () -> {
              List<Schema.Field> modifiedFields = new ArrayList<>(fields);
              modifiedFields.add(newField);
              Schema.createRecord(
                  schema.getName(),
                  schema.getDoc(),
                  schema.getNamespace(),
                  schema.isError(),
                  modifiedFields);
            });
    assertThat(e1.getMessage(), is("Field already used: id type:LONG pos:0"));

    // You have to clone all of the existing fields into the new list.
    List<Schema.Field> modifiedFields = new ArrayList<>();
    for (Schema.Field f : schema.getFields()) {
      // Cloning a field means copying all of its members, and it's metadata.
      Schema.Field newF;
      if (AvroVersion.avro_1_9.orAfter()) newF = new Schema.Field(f, f.schema());
      else {
        newF = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
        for (Map.Entry<String, Object> kv : f.getObjectProps().entrySet())
          newF.addProp(kv.getKey(), kv.getValue());
        if (f.aliases() != null) for (String alias : f.aliases()) newF.addAlias(alias);
      }
      modifiedFields.add(newF);
    }
    modifiedFields.add(newField);

    // Now you can clone the original record schema.
    Schema modifiedSchema =
        Schema.createRecord(
            schema.getName(),
            schema.getDoc(),
            schema.getNamespace(),
            schema.isError(),
            modifiedFields);
    if (AvroVersion.avro_1_9.orAfter()) modifiedSchema.addAllProps(schema);
    else
      for (Map.Entry<String, Object> kv : schema.getObjectProps().entrySet())
        modifiedSchema.addProp(kv.getKey(), kv.getValue());

    assertThat(modifiedSchema.getFields(), hasSize(3));
  }
}
