package com.skraba.avro.enchiridion.core.extra;

import org.junit.jupiter.api.Nested;

/** This class exists just to aggregate other unit tests into other maven modules. */
public class Aggregated {

  @Nested
  public class SimpleJiraTest extends com.skraba.avro.enchiridion.core.extra.SimpleJiraTest {}

  @Nested
  public class ClassValidationSecurityTest
      extends com.skraba.avro.enchiridion.core.extra.ClassValidationSecurityTest {}
}
