package com.skraba.avro.enchiridion.core.schema;

import org.junit.jupiter.api.Nested;

/** This class exists just to aggregate other unit tests into other maven modules. */
public class Aggregated {

  @Nested
  public class BuildersTest extends com.skraba.avro.enchiridion.core.schema.BuildersTest {}

  @Nested
  public class FingerprintTest extends com.skraba.avro.enchiridion.core.schema.FingerprintTest {}

  @Nested
  public class SchemaManipulationTest
      extends com.skraba.avro.enchiridion.core.schema.SchemaManipulationTest {}

  @Nested
  public class UserPropertiesTest
      extends com.skraba.avro.enchiridion.core.schema.UserPropertiesTest {}
}
