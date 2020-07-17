package com.skraba.avro.enchiridion.junit;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

import com.skraba.avro.enchiridion.core.AvroVersion;
import java.util.Optional;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * {@link ExecutionCondition} for {@link EnabledForAvroVersion @EnabledForAvroVersion}.
 *
 * @see EnabledForAvroVersion
 */
public class EnabledForAvroVersionCondition implements ExecutionCondition {

  private static final ConditionEvaluationResult ENABLED_BY_DEFAULT =
      enabled("@EnabledForAvroVersion is not present");

  static final ConditionEvaluationResult ENABLED_ON_CURRENT_VERSION = //
      enabled("Enabled for " + AvroVersion.getInstalledAvro());

  static final ConditionEvaluationResult DISABLED_ON_CURRENT_VERSION = //
      enabled("Disabled for " + AvroVersion.getInstalledAvro());

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    Optional<EnabledForAvroVersion> x =
        findAnnotation(context.getElement(), EnabledForAvroVersion.class);
    //    Object y =
    //        x.map(annotation -> annotation.startingFrom().orAfter() &&
    // annotation.until().before());
    AvroVersion current = AvroVersion.getInstalledAvro();
    Boolean both = null;
    if (x.isPresent()) {
      boolean orAfter = x.get().startingFrom().orAfter();
      boolean before = x.get().until().before();
      both = orAfter && before;
    }

    return findAnnotation(context.getElement(), EnabledForAvroVersion.class)
        .map(annotation -> annotation.startingFrom().orAfter() && annotation.until().before())
        .map(enabled -> enabled ? ENABLED_ON_CURRENT_VERSION : DISABLED_ON_CURRENT_VERSION)
        .orElse(ENABLED_BY_DEFAULT);
  }
}
