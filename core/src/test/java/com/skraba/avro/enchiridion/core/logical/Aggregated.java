package com.skraba.avro.enchiridion.core.logical;

import org.junit.jupiter.api.Nested;

/** This class exists just to aggregate other unit tests into other maven modules. */
public class Aggregated {

  @Nested
  public class DateAndTimeTests extends com.skraba.avro.enchiridion.core.logical.DateAndTimeTests {}

  @Nested
  public class DecimalPrecisionAndScaleTest
      extends com.skraba.avro.enchiridion.core.logical.DecimalPrecisionAndScaleTest {}

  @Nested
  public class DecimalTest extends com.skraba.avro.enchiridion.core.logical.DecimalTest {}
}
