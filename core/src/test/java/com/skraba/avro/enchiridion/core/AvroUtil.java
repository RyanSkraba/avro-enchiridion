package com.skraba.avro.enchiridion.core;

import java.math.BigDecimal;
import java.util.List;
import org.apache.avro.Schema;

/**
 * Collector of helper methods.
 *
 * <p>No significant logic here, just repetitive methods that can be expressed clearly in a single
 * line.
 */
public class AvroUtil {

  public static ThreadLocal<SchemaApi> api = ThreadLocal.withInitial(SchemaApi::new);

  /** Get an instance that wraps some Avro SDK methods that have evolved between versions. */
  public static SchemaApi api() {
    return api.get();
  }

  /** Print information about a Java BigDecimal. */
  public static void pbd(BigDecimal bd) {
    System.out.println(
        "BigDecimal(" + bd.precision() + ":" + bd.scale() + ":" + bd.toString() + ")");
  }

  public static String jsonify(Object defaultVal) {
    return defaultVal instanceof CharSequence
        ? "\"" + defaultVal + '"'
        : String.valueOf(defaultVal);
  }

  /**
   * This class provides a little indirection to methods that may or may not exist in different
   * versions of the Avro Schema API.
   */
  public static class SchemaApi {

    /**
     * See {@link org.apache.avro.Schema#createRecord(java.lang.String, java.lang.String,
     * java.lang.String, boolean, java.util.List)}
     */
    public Schema createRecord(
        String name, String namespace, String doc, boolean isError, List<Schema.Field> fields) {
      return Schema.createRecord(name, namespace, doc, isError, fields);
    }

    public Schema.Field createField(String name, Schema schema, String doc, Object defaultValue) {
      return createField(name, schema, doc, defaultValue, Schema.Field.Order.ASCENDING);
    }

    public Schema.Field createField(
        String name, Schema schema, String doc, Object defaultValue, Schema.Field.Order order) {
      return new Schema.Field(name, schema, doc, defaultValue, order);
    }

    public Schema parse(String jsonString) {
      return new Schema.Parser().parse(jsonString);
    }
  }
}
