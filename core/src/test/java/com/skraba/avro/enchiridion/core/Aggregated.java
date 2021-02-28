package com.skraba.avro.enchiridion.core;

import org.junit.jupiter.api.Nested;

/** This class exists just to aggregate other unit tests into other maven modules. */
public class Aggregated {

  static {
    System.out.println("==========");
    System.out.println("Running aggregated tests with " + AvroVersion.getInstalledAvro());
    System.out.println("==========");
  }

  //  @Test
  //  public void testAvroVersion() {
  //    assertThat(AvroVersion.avro_1_11.before("Next major version"), is(true));
  //    assertThat(AvroVersion.avro_1_10.orAfter("This major version"), is(true));
  //    assertThat(AvroVersion.getInstalledAvro(), is(AvroVersion.avro_1_10));
  //  }

  @Nested
  public class GenericDataTest extends com.skraba.avro.enchiridion.core.GenericDataTest {}

  @Nested
  public class SerializeToBytesTest extends com.skraba.avro.enchiridion.core.SerializeToBytesTest {}

  @Nested
  public class SerializeToJsonTest extends com.skraba.avro.enchiridion.core.SerializeToJsonTest {}

  @Nested
  public class SerializeToMessageTest
      extends com.skraba.avro.enchiridion.core.SerializeToMessageTest {}

  @Nested
  public class SimpleJiraTest extends com.skraba.avro.enchiridion.core.SimpleJiraTest {}

  @Nested
  public class EvolutionAggregated extends com.skraba.avro.enchiridion.core.evolution.Aggregated {}

  @Nested
  public class FileAggregated extends com.skraba.avro.enchiridion.core.file.Aggregated {}

  @Nested
  public class LogicalAggregated extends com.skraba.avro.enchiridion.core.logical.Aggregated {}

  @Nested
  public class SchemaAggregated extends com.skraba.avro.enchiridion.core.schema.Aggregated {}
}
