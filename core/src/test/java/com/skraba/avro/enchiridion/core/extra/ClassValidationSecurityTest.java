package com.skraba.avro.enchiridion.core.extra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.skraba.avro.enchiridion.core.SerializeToBytesTest;
import com.skraba.avro.enchiridion.core.SpecificDataTest;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Test;

public class ClassValidationSecurityTest {

  private static final Schema DANGER_SCHEMA =
      SchemaBuilder.builder()
          .record(Danger.class.getName())
          .fields()
          .requiredString("sensitive")
          .endRecord();

  private static final GenericRecord DANGER_RECORD =
      new GenericRecordBuilder(DANGER_SCHEMA).set("sensitive", "EXPOSED").build();

  private static final byte[] DANGER_BYTES =
      SerializeToBytesTest.toBytes(GenericData.get(), DANGER_SCHEMA, DANGER_RECORD);

  @Test
  public void testSerializationIsOk() {
    // Serializing the record doesn't cause any instance of our Danger class to be instantiated
    var bytes1 = SerializeToBytesTest.toBytes(GenericData.get(), DANGER_SCHEMA, DANGER_RECORD);
    var bytes2 = SpecificDataTest.toBytes(SpecificData.get(), DANGER_SCHEMA, DANGER_RECORD);
    assertThat(bytes1).isEqualTo(DANGER_BYTES);
    assertThat(bytes2).isEqualTo(DANGER_BYTES);
  }

  @Test
  public void testSpecificDataWontInstantiate() {
    if (AvroVersion.avro_1_13.orAfter(
        "Tightening up instantiation.  TODO: Apply to 1.12 and 1.13")) {
      assertThatThrownBy(
              () -> SpecificDataTest.fromBytes(SpecificData.get(), DANGER_SCHEMA, DANGER_BYTES))
          .isInstanceOf(SecurityException.class)
          .hasMessageStartingWith("Forbidden com.skraba.avro.enchiridion.core.extra.Danger!");
    }
  }
}
