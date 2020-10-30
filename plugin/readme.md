The Avro maven plugin
=====================

```xml
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro-maven-plugin</artifactId>
        <version>${avro.version}</version>
        <executions>
          <execution>
            <phase>generate-sources</phase>
            <goals>
              <goal>schema</goal>
              <goal>protocol</goal>
              <goal>idl-protocol</goal>
            </goals>
            <configuration>
              <fieldVisibility>public</fieldVisibility>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
```

SpecificCompiler options
------------------------

[avro-tools][avro-tools] | [Plugin][avro-maven-plugin] | [SpecificCompiler][avro-specific-compiler] properties and accessors | Description 
--- | --- | --- | ---
x | **customConversions** = `new String[0]` | **specificData**<br/>`sc.addLogicalTypeConversions(SpecificData)` <br/> `sc.addCustomConversion(Class<?>)`<br/>`sc.getUsedConversionClasses()`   |
x | **customLogicalTypeFactories** = `new String[0]` | | LogicalTypes.register(...)
x | x | **queue**                                                                                                                                        |
x | x | **protocol**                                                                                                                                     |                                                                                                                                            
x | x | **velocityEngine**                                                                                                                               |
x | velocityToolsClassesNames = `new String[0]` | **additionalVelocityTools**<br/>`sc.setAdditionalVelocityTools(List<Object>)`                                                                    | Puts instances of any object into the velocity context (references by their simple class name). 
x | templateDirectory = `"/org/apache/avro/compiler/specific/templates/java/classic/"` | **templateDir = `"/org/apache/avro/compiler/specific/templates/java/classic/"`**<br/>_(Overridden by `org.apache.avro.specific.templates`)_<br/>`sc.setTemplateDir(String)`                                                                                  | Where to find the velocity templates. 
`[-fieldVisibility <visibilityType>]` | fieldVisibility = `private` | **fieldVisibility = `PRIVATE`**<br/>`sc.setFieldVisibility(FieldVisibility)`<br/>`sc.deprecatedFields()`<br/>`sc.publicFields()`<br/>`sc.privateFields()` | Record fields  should be `public` `@Deprecated public`, or `private` 
x | createSetters = `true` | **createSetters = `true`****<br/>`sc.isCreateSetters()`<br/>`sc.setCreateSetters(boolean)`                                                       | Whether to create setters for fields of the record. 
x | createOptionalGetters = `false` | **createOptionalGetters = `false`****<br/>`sc.isCreateOptionalGetters()`<br/>`sc.setCreateOptionalGetters(boolean)`                              | Whether to create additional getters `getOptionalMyField()` for fields of the record. 
x | gettersReturnOptional = `false` | **gettersReturnOptional = `false`**<br/>`sc.isGettersReturnOptional()`<br/>`sc.setGettersReturnOptional(boolean)`                                | Whether the getters return `Optional<...>` instead of possibly null. 
x | optionalGettersForNullableFieldsOnly = `false` | **optionalGettersForNullableFieldsOnly = `false`**<br/>`sc.isOptionalGettersForNullableFieldsOnly()`<br/>`sc.setOptionalGettersForNullableFieldsOnly(boolean)` | If `gettersReturnOptional`, only if they are actually nullable. 
x | enableDecimalLogicalType = `false` | **enableDecimalLogicalType = `false`**<br/>`sc.setEnableDecimalLogicalType()`                                                                    | Whether to create setters for fields of the record. 
x | x | **createAllArgsConstructor = `true`**<br>`sc.isCreateAllArgsConstructor`                                                                         | If possible, create a constructor with every field.
`[-encoding <outputencoding>]` | x | **outputCharacterEncoding**<br/>`sc.setOutputCharacterEncoding(String)`                                                                          |
`[-bigDecimal]` | x | **enableDecimalLogicalType**<br/>`sc.setEnableDecimalLogicalType`                                                                                | Whether to use the Decimal type 
x | x | **suffix = `.java`**<br/>`sc.setSuffix(String)`                                                                                                  | 
`-string` to use `String`, else `CharSequence` | stringType = "CharSequence" | **stringType = `CharSequence`**<br/>`setStringType(StringType)`                                                                                  | One of `CharSequence`, `String`, `Utf8`
`input` | sourceDirectory = `"${basedir}/src/main/avro"` | | 
`outputdir` | outputDirectory = `"${project.build.directory}/generated-sources/avro"` | | 
x | testSourceDirectory = `"${basedir}/src/main/avro"` | | 
x | testOutputDirectory = `"${project.build.directory}/generated-sources/avro"` | | 
x | imports | | 
x | excludes = `new String[0]` | | 
x | testExcludes = `new String[0]` | | 
x | includes = `new String[] { "**/*.avsc" }` | | SchemaMojo (`.avpr` for ProtocolMojo) 
x | testIncludes = `new String[] { "**/*.avsc" }` | | SchemaMojo (`.avpr` for ProtocolMojo)

### TODO

* Add [AVRO-2937](https://issues.apache.org/jira/browse/AVRO-2937) to this list.

* Apply a same order
* Align the documentation and defaults
* Maven plugin defaults sometimes in parameter sometimes set?
* Maven plugin testSourceDirectory have property set to sourceDirectory
* `" -string - use java.lang.String instead of Utf8"` is wrong ?

[avro-tools]: https://github.com/apache/avro/blob/master/lang/java/tools/src/main/java/org/apache/avro/tool/SpecificCompilerTool.java
[avro-maven-plugin]: https://github.com/apache/avro/blob/master/lang/java/maven-plugin/src/main/java/org/apache/avro/mojo/AbstractAvroMojo.java
[avro-specific-compiler]: https://github.com/apache/avro/blob/master/lang/java/compiler/src/main/java/org/apache/avro/compiler/specific/SpecificCompiler.java