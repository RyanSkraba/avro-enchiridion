package com.skraba.avro.enchiridion.core;

import java.math.BigDecimal;
import org.apache.avro.Schema;

/**
 * Collector of helper methods.
 *
 * <p>No significant logic here, just repetitive methods that can be expressed clearly in a single
 * line.
 */
public class AvroUtil {

  public static Schema parseSchema(String jsonString) {
    return new Schema.Parser().parse(jsonString);
  }

  public static void pbd(BigDecimal bd) {
    System.out.println(bd.precision() + ":" + bd.scale() + ":" + bd.toString());
  }
}
