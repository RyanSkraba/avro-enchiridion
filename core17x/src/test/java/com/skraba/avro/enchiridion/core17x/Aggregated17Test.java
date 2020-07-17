package com.skraba.avro.enchiridion.core17x;

import com.skraba.avro.enchiridion.core.Aggregated;
import com.skraba.avro.enchiridion.core.AvroUtil;
import java.util.List;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Nested;

public class Aggregated17Test extends Aggregated {

  static {
    AvroUtil.api = ThreadLocal.withInitial(SchemaApi17x::new);
  }

  /** Logical types do not exist in Avro 1.7. Overriding the nested test causes it to be skipped. */
  @Nested
  public class LogicalAggregated {}

  @Nested
  public class SchemaAggregated extends com.skraba.avro.enchiridion.core.schema.Aggregated {
    /** User properties were handled only as Jackson JsonNode in 1.7.x */
    @Nested
    public class UserPropertiesTest {}
  }

  @Nested
  public class EvolutionAggregated extends com.skraba.avro.enchiridion.core.evolution.Aggregated {

    @Nested
    public class EvolveRemoveAFieldTest {}

    @Nested
    public class EvolveRenameAFieldTest {}

    @Nested
    public class EvolveReorderFieldsTest {}
  }

  /** Some of the methods tested need to be adapted to Avro 1.7 */
  private static class SchemaApi17x extends AvroUtil.SchemaApi {
    @Override
    public Schema createRecord(
        String name, String namespace, String doc, boolean isError, List<Schema.Field> fields) {
      Schema record = Schema.createRecord(name, namespace, doc, isError);
      record.setFields(fields);
      return record;
    }

    @Override
    public Schema.Field createField(
        String name, Schema schema, String doc, Object defaultValue, Schema.Field.Order order) {
      JsonNode defaultJsonNode = new ObjectMapper().valueToTree(defaultValue);
      return new Schema.Field(name, schema, doc, defaultJsonNode, order);
    }
  }
}
