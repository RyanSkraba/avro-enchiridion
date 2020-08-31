package com.skraba.avro.enchiridion.junit;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

import com.skraba.avro.enchiridion.core.AvroVersion;
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

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    return findAnnotation(context.getElement(), EnabledForAvroVersion.class)
        .map(
            annotation -> {
              if (annotation.startingFrom().orAfter() && annotation.until().before()) {
                return ENABLED_ON_CURRENT_VERSION;
              } else {
                return disabled(
                    "Disabled for " + AvroVersion.getInstalledAvro() + ": " + annotation.reason());
              }
            })
        .orElse(ENABLED_BY_DEFAULT);
  }
}
