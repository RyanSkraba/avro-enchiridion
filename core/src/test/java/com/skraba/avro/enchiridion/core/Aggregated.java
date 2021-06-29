package com.skraba.avro.enchiridion.core;

import org.junit.jupiter.api.Nested;

/** This class exists just to aggregate other unit tests into other maven modules. */
public class Aggregated {

  static {
    System.out.println("==========");
    System.out.println("Running aggregated tests with " + AvroVersion.getInstalledAvro());
    System.out.println("==========");
  }

  @Nested
  public class GenericDataTest extends com.skraba.avro.enchiridion.core.GenericDataTest {}

  @Nested
  public class ReflectDataTest extends com.skraba.avro.enchiridion.core.ReflectDataTest {}

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
  public class SpecificDataTest extends com.skraba.avro.enchiridion.core.SpecificDataTest {}

  @Nested
  public class EvolutionAggregated extends com.skraba.avro.enchiridion.core.evolution.Aggregated {}

  @Nested
  public class FileAggregated extends com.skraba.avro.enchiridion.core.file.Aggregated {}

  @Nested
  public class LogicalAggregated extends com.skraba.avro.enchiridion.core.logical.Aggregated {}

  @Nested
  public class SchemaAggregated extends com.skraba.avro.enchiridion.core.schema.Aggregated {}
}
