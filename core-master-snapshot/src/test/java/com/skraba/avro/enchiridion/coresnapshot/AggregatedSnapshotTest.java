package com.skraba.avro.enchiridion.coresnapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.core.Aggregated;
import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
import java.io.Closeable;
import org.apache.avro.util.ClassSecurityValidator;
import org.junit.jupiter.api.Test;

public class AggregatedSnapshotTest extends Aggregated {

  static {
    AvroUtil.api = ThreadLocal.withInitial(ApiCompatibility113x::new);
  }

  @Test
  public void testAvroVersion() {
    assertThat(AvroVersion.avro_infinity.before("Next major version"), is(true));
    assertThat(AvroVersion.avro_1_13.orAfter("This major version"), is(true));
    assertThat(AvroVersion.getInstalledAvro(), is(AvroVersion.avro_1_13));
  }

  /** Some of the methods tested need to be adapted to Avro 1.9 */
  private static class ApiCompatibility113x extends AvroUtil.ApiCompatibility {
    /**
     * Sets the classes as trusted serializable classes temporarily.
     *
     * @param trusted New classes to trust in serialization
     * @return A closeable that resets the trusted serializable classes
     */
    @Override
    public Closeable trustClasses(Class<?>... trusted) {
      var oldGlobal = ClassSecurityValidator.getGlobal();
      var trustBuilder = ClassSecurityValidator.builder();
      for (var cls : trusted) {
        trustBuilder.add(cls);
      }
      ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(trustBuilder.build()));
      return () -> ClassSecurityValidator.setGlobal(oldGlobal);
    }
  }
}
