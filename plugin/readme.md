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