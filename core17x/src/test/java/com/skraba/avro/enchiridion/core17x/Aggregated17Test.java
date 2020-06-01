package com.skraba.avro.enchiridion.core17x;

import com.skraba.avro.enchiridion.core.Aggregated;
import org.junit.jupiter.api.Nested;

public class Aggregated17Test extends Aggregated {

  /** Logical types do not exist in Avro 1.7. Overriding the nested test causes it to be skipped. */
  @Nested
  public class LogicalAggregated {}
}
