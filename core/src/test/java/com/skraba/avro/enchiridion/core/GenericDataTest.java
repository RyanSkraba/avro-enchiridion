package com.skraba.avro.enchiridion.core;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.roundTripBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.skraba.avro.enchiridion.resources.AvroTestResources;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
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

  @Test
  public void testNumbers() {
    Schema simple = api().parse(AvroTestResources.SimpleRecord());

    GenericRecordBuilder grb = new GenericRecordBuilder(simple).set("name", "One");

    GenericRecord rLong = grb.set("id", 1L).build();
    GenericRecord rByte = grb.set("id", (byte) 1).build();
    GenericRecord rShort = grb.set("id", (short) 1).build();
    GenericRecord rInt = grb.set("id", 1).build();
    GenericRecord rFloat = grb.set("id", 1.0f).build();
    GenericRecord rDouble = grb.set("id", 1.0d).build();

    assertTrue(GenericData.get().validate(simple, rLong));
    assertFalse(GenericData.get().validate(simple, rByte));
    assertFalse(GenericData.get().validate(simple, rShort));
    assertFalse(GenericData.get().validate(simple, rInt));
    assertFalse(GenericData.get().validate(simple, rFloat));
    assertFalse(GenericData.get().validate(simple, rDouble));

    assertThrows(ClassCastException.class, () -> rLong.equals(rByte));
    assertThrows(ClassCastException.class, () -> rShort.equals(rLong));
    assertThrows(ClassCastException.class, () -> rInt.equals(rLong));
    assertThrows(ClassCastException.class, () -> rFloat.equals(rLong));
    assertThrows(ClassCastException.class, () -> rDouble.equals(rLong));

    assertThat(roundTripBytes(simple, rLong), is(rLong));
    if (AvroVersion.avro_1_10.orAfter("Change serialization behaviour AVRO-2070")) {
      assertThat(roundTripBytes(simple, rByte), is(rLong));
      assertThat(roundTripBytes(simple, rShort), is(rLong));
      assertThat(roundTripBytes(simple, rInt), is(rLong));
      assertThat(roundTripBytes(simple, rFloat), is(rLong));
      assertThat(roundTripBytes(simple, rDouble), is(rLong));
    } else {
      assertThrows(ClassCastException.class, () -> roundTripBytes(simple, rByte));
      assertThrows(ClassCastException.class, () -> roundTripBytes(simple, rShort));
      assertThrows(ClassCastException.class, () -> roundTripBytes(simple, rInt));
      assertThrows(ClassCastException.class, () -> roundTripBytes(simple, rFloat));
      assertThrows(ClassCastException.class, () -> roundTripBytes(simple, rDouble));
    }
  }
}
