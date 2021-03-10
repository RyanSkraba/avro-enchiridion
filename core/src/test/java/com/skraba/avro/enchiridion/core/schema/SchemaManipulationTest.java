package com.skraba.avro.enchiridion.core.schema;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.core.AvroVersion;
import com.skraba.avro.enchiridion.junit.EnabledForAvroVersion;
import com.skraba.avro.enchiridion.resources.AvroTestResources;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

/** Changing a schema programmatically can be complicated in Avro. */
public class SchemaManipulationTest {

  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_8,
      reason = "Avro API requires Jackson classes to create fields.")
  public void testAddAFieldToARecord() {

    // We have an original record.
    Schema schema = api().parse(AvroTestResources.SimpleRecord());
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
      if (AvroVersion.avro_1_9.orAfter("Field constructor added in Avro 1.9.x"))
        newF = new Schema.Field(f, f.schema());
      else {
        newF = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
        for (Map.Entry<String, Object> kv : f.getObjectProps().entrySet())
          newF.addProp(kv.getKey(), kv.getValue());
        if (f.aliases() != null) for (String alias : f.aliases()) newF.addAlias(alias);
      }
      modifiedFields.add(newF);
    }
    modifiedFields.add(newField);

    // This is the fast way to clone a list of fields using the copy constructor after Avro 1.9.x
    if (AvroVersion.avro_1_9.orAfter("Field constructor added in Avro 1.9.x")) {
      List<Schema.Field> clonedFields =
          schema.getFields().stream()
              .map(f -> new Schema.Field(f, f.schema()))
              .collect(Collectors.toList());
      assertThat(clonedFields, is(schema.getFields()));
    }

    // Now you can clone the original record schema.
    Schema modifiedSchema =
        Schema.createRecord(
            schema.getName(),
            schema.getDoc(),
            schema.getNamespace(),
            schema.isError(),
            modifiedFields);
    if (AvroVersion.avro_1_9.orAfter("addAllProps added in Avro 1.9.x"))
      modifiedSchema.addAllProps(schema);
    else
      for (Map.Entry<String, Object> kv : schema.getObjectProps().entrySet())
        modifiedSchema.addProp(kv.getKey(), kv.getValue());

    assertThat(modifiedSchema.getFields(), hasSize(3));
  }

  /** Given two records, merge all of the fields into one record. */
  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_8,
      reason = "Avro API requires Jackson classes to create fields.")
  public void testFlatMergeTwoSchemas() {
    Schema recordA =
        SchemaBuilder.builder()
            .record("A")
            .fields()
            .requiredLong("a1")
            .requiredLong("a2")
            .requiredLong("a3")
            .endRecord();
    Schema recordB =
        SchemaBuilder.builder()
            .record("B")
            .fields()
            .requiredLong("b1")
            .requiredLong("b2")
            .endRecord();

    // Merge by copying fields to a new list and using that to construct the new record.
    List<Schema.Field> merged = new ArrayList<>();
    for (Schema.Field f : recordA.getFields()) merged.add(api().createField(f, f.schema()));
    for (Schema.Field f : recordB.getFields()) merged.add(api().createField(f, f.schema()));
    Schema recordAB = Schema.createRecord("AB", null, null, false);
    recordAB.setFields(merged);

    assertThat(recordAB.getFields(), hasSize(5));
  }

  /**
   * Copy all annotations from the source schema to the destination schema. Errors are not handled
   * if the two schemas are not structurally identical except for schemas.
   *
   * @param src A schema with annotations that we want to apply to the destination.
   * @param dst A schema to receive new annotations. Unlike any other Avro schema transformation,
   *     annotations are mutable and this instance will be changed after the method call.
   */
  public void copyAnnotations(Schema src, Schema dst) {
    // Copy all of the annotations directly on the src schema.
    for (Map.Entry<String, Object> e : src.getObjectProps().entrySet())
      // Check if the destination already has that property, or this will fail.
      if (dst.getProp(e.getKey()) == null) dst.addProp(e.getKey(), e.getValue());

    // Note that Avro 1.9.x adds the dst.putAll(src) method to simplify. Both getObjectProps
    // loops can be avoided.

    switch (src.getType()) {
      case RECORD:
        for (Schema.Field srcF : src.getFields()) {
          Schema.Field dstF = dst.getField(srcF.name());
          // Copy all of the field annotations.
          for (Map.Entry<String, Object> e : srcF.getObjectProps().entrySet())
            if (dstF.getProp(e.getKey()) == null) dstF.addProp(e.getKey(), e.getValue());
          // Then recursively copy the field's schema annotations.
          copyAnnotations(srcF.schema(), dstF.schema());
        }
        break;
      case ARRAY:
        copyAnnotations(src.getElementType(), dst.getElementType());
        break;
      case MAP:
        copyAnnotations(src.getValueType(), dst.getValueType());
        break;
      case UNION:
        for (int i = 0; i < src.getTypes().size(); i++)
          copyAnnotations(src.getTypes().get(i), dst.getTypes().get(i));
        break;
      default:
        // No additional annotations to copy
        break;
    }
  }

  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_8,
      reason = "Avro API requires Jackson classes for annotations.")
  public void testAnnotateSchema() {

    // A simple nested record.
    Schema recordA =
        SchemaBuilder.builder()
            .record("A")
            .fields()
            .requiredLong("a1")
            .name("a2")
            .type()
            .record("B")
            .fields()
            .requiredLong("b1")
            .name("b2")
            .type()
            .unionOf()
            .stringType()
            .and()
            .intType()
            .endUnion()
            .noDefault()
            .endRecord()
            .noDefault()
            .endRecord();

    // A clone of the record, with an annotation on all fields and schemas.
    Schema recordAnnotated =
        SchemaBuilder.builder()
            .record("A")
            .fields()
            .requiredLong("a1")
            .name("a2")
            .type()
            .record("B")
            .fields()
            .requiredLong("b1")
            .name("b2")
            .type()
            .unionOf()
            .stringType()
            .and()
            .intType()
            .endUnion()
            .noDefault()
            .endRecord()
            .noDefault()
            .endRecord();
    {
      recordAnnotated.addProp("A", true);
      recordAnnotated.getField("a1").addProp("A.a1", true);
      recordAnnotated.getField("a1").schema().addProp("A.a1.schema", true);
      recordAnnotated.getField("a2").addProp("A.a2", true);
      recordAnnotated.getField("a2").schema().addProp("A.a2.schema", true);

      Schema recordB = recordAnnotated.getField("a2").schema();
      recordB.getField("b1").addProp("A.a2.b1", true);
      recordB.getField("b1").schema().addProp("A.a2.b1.schema", true);
      recordB.getField("b2").addProp("A.a2.b2", true);
      recordB.getField("b2").schema().addProp("A.a2.b2.schema", true);

      Schema b2Union = recordB.getField("b2").schema();
      b2Union.getTypes().get(0).addProp("A.a2.b2.schema[0]", true);
      b2Union.getTypes().get(1).addProp("A.a2.b2.schema[1]", true);
    }

    // Assert that the two records are no longer identical.
    assertThat(recordA, not(recordAnnotated));

    // Copy the annotations from one record back into another.
    copyAnnotations(recordAnnotated, recordA);
    assertThat(recordA.toString(true), is(recordAnnotated.toString(true)));
  }
}
