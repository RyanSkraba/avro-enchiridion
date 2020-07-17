package com.skraba.avro.enchiridion.core.evolution;

import org.junit.jupiter.api.Nested;

/** This class exists just to aggregate other unit tests into other maven modules. */
public class Aggregated {

  @Nested
  public class DefaultsTest extends com.skraba.avro.enchiridion.core.evolution.DefaultsTest {}

  @Nested
  public class EvolveAddAFieldTest
      extends com.skraba.avro.enchiridion.core.evolution.EvolveAddAFieldTest {}

  @Nested
  public class EvolveRemoveAFieldTest
      extends com.skraba.avro.enchiridion.core.evolution.EvolveRemoveAFieldTest {}

  @Nested
  public class EvolveRenameAFieldTest
      extends com.skraba.avro.enchiridion.core.evolution.EvolveRenameAFieldTest {}

  @Nested
  public class EvolveReorderFieldsTest
      extends com.skraba.avro.enchiridion.core.evolution.EvolveReorderFieldsTest {}
}
