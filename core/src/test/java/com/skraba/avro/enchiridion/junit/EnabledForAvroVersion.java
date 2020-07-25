package com.skraba.avro.enchiridion.junit;

import com.skraba.avro.enchiridion.core.AvroVersion;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@code @EnabledOnAvroVersion} is used to signal that the annotated test class or test method is
 * only <em>enabled</em> for the specified {@link AvroVersion} libraries.
 *
 * <p>When applied at the class level, all test methods within that class will be enabled.
 *
 * @see EnabledForAvroVersionCondition
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(EnabledForAvroVersionCondition.class)
public @interface EnabledForAvroVersion {

  /** The first version of Avro where this test should start being enabled. */
  AvroVersion startingFrom();

  /** Why this test is disabled for some versions. */
  String reason();

  /** The version of Avro where this test should stop being enabled. */
  AvroVersion until() default AvroVersion.avro_infinity;
}
