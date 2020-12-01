package com.skraba.avro.enchiridion.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.io.FileMatchers.anExistingFile;

import com.skraba.avro.enchiridion.idl.TimestampAll;
import com.skraba.avro.enchiridion.recipe.Recipe;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SpecificCompilerTest {

  @Test
  public void testBasicGeneration(@TempDir Path tmpDir) throws IOException {
    SpecificCompiler sc = new SpecificCompiler(Recipe.getClassSchema());
    sc.setFieldVisibility(SpecificCompiler.FieldVisibility.PUBLIC);
    sc.setStringType(GenericData.StringType.String);
    sc.compileToDestination(null, tmpDir.toFile());
    assertThat(
        tmpDir.resolve("com/skraba/avro/enchiridion/recipe/Recipe.java").toFile(),
        anExistingFile());
  }

  @Test
  public void testCompilingLogicalTypeWithUnion(@TempDir Path tmpDir) throws IOException {
    SpecificCompiler sc = new SpecificCompiler(TimestampAll.getClassSchema());
    sc.setFieldVisibility(SpecificCompiler.FieldVisibility.PUBLIC);
    sc.setStringType(GenericData.StringType.String);
    sc.compileToDestination(null, tmpDir.toFile());
    assertThat(
        tmpDir.resolve("com/skraba/avro/enchiridion/idl/TimestampAll.java").toFile(),
        anExistingFile());
  }
}
