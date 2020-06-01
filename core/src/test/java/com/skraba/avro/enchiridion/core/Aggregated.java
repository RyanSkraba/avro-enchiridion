package com.skraba.avro.enchiridion.core;

import org.junit.jupiter.api.Nested;

/** This class exists just to aggregate other unit tests into other maven modules. */
public class Aggregated {

  static {
    System.out.println("Running aggregated tests with " + AvroVersion.getInstalledAvro());
  }

  @Nested
  public class SimpleJiraTest extends com.skraba.avro.enchiridion.core.SimpleJiraTest {}

  @Nested
  public class SerializeToBytesTest extends com.skraba.avro.enchiridion.core.SerializeToBytesTest {}

  @Nested
  public class FileAggregated extends com.skraba.avro.enchiridion.core.file.Aggregated {}

  @Nested
  public class LogicalAggregated extends com.skraba.avro.enchiridion.core.logical.Aggregated {}
}
