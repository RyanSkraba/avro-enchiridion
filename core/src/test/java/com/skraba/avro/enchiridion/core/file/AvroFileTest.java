package com.skraba.avro.enchiridion.core.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.io.FileMatchers.aFileNamed;
import static org.hamcrest.io.FileMatchers.aFileWithSize;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.resources.AvroTestResources;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
import com.skraba.avro.enchiridion.testkit.EnabledForAvroVersion;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.RandomData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for Avro files and containers.
 *
 * <p>{@link DataFileWriter}
 */
public class AvroFileTest {

  /**
   * Writes exactly one datum to a file.
   *
   * @param f The file to create or write to.
   * @param model The GenericData model to use for interpreting the datum.
   * @param schema The schema corresponding to a datum.
   * @param datum The actual value to write.
   * @param <T> The type of the datum being written.
   * @throws IOException If there was an error communicating with the file.
   */
  public static <T> void toFile(File f, GenericData model, Schema schema, T datum)
      throws IOException {
    try (DataFileWriter<T> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema, model))) {
      // writer.setCodec(CodecFactory.snappyCodec());
      writer.create(schema, f);
      writer.append(datum);
    }
  }

  /**
   * Read exactly one datum from a file.
   *
   * @param f The file to read from.
   * @param model The GenericData model to use for interpreting the datum.
   * @param <T> The type of the datum being written.
   * @throws IOException If there was an error communicating with the file.
   */
  public static <T> T fromFile(File f, GenericData model) throws IOException {
    // Using a null reader/writer schema will read the schema from the file.
    try (DataFileReader<T> reader =
        new DataFileReader<>(f, new GenericDatumReader<>(null, null, model))) {
      return reader.next(null);
    }
  }

  @Test
  public void testRoundTripSerializeIntegerToFile(@TempDir Path tmpDir) throws IOException {
    Schema schema = SchemaBuilder.builder().intType();

    // From an int to the file.
    File f = tmpDir.resolve("integer.avro").toFile();
    toFile(f, GenericData.get(), schema, 1_234_567);

    assertThat(f, anExistingFile());
    assertThat(f, aFileNamed(equalToIgnoringCase("integer.avro")));
    assertThat(f, aFileWithSize(62L));

    Integer datum = fromFile(f, GenericData.get());
    assertThat(datum, is(1_234_567));
  }

  @Test
  public void testAppendToFile(@TempDir Path tmpDir) throws IOException {
    Schema schema = AvroUtil.api().parse(AvroTestResources.SimpleRecord());
    File f = tmpDir.resolve("simple.avro").toFile();

    GenericRecord one = new GenericRecordBuilder(schema).set("id", 1L).set("name", "one").build();
    GenericRecord two = new GenericRecordBuilder(schema).set("id", 2L).set("name", "two").build();
    GenericRecord three =
        new GenericRecordBuilder(schema).set("id", 3L).set("name", "three").build();

    // Write a single record to the file
    toFile(f, GenericData.get(), schema, one);
    assertThat(f, anExistingFile());
    assertThat(f, aFileNamed(equalToIgnoringCase("simple.avro")));
    assertThat(f, aFileWithSize(251L));

    // Append the new records.
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(schema, GenericData.get()))) {
      writer.appendTo(f);
      writer.append(two);
      writer.append(three);
    }
    assertThat(f, aFileWithSize(281L));

    // The two records are in the file.
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(f, new GenericDatumReader<>(null, null, GenericData.get()))) {
      assertThat(reader.next(null), is(one));
      assertThat(reader.next(null), is(two));
      assertThat(reader.next(null), is(three));
      assertThrows(NoSuchElementException.class, reader::next);
    }

    // Internally, the original write and the append are each stored in their own block.
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(f, new GenericDatumReader<>(null, null, GenericData.get()))) {
      // The original block contains one record.
      ByteBuffer b = reader.nextBlock();
      assertThat(b.position(), is(0));
      assertThat(b.remaining(), is(5));
      assertThat(reader.getBlockCount(), is(1L));
      // The second block contains the two appeneded records.
      b = reader.nextBlock();
      assertThat(b.position(), is(0));
      assertThat(b.remaining(), is(12));
      assertThat(reader.getBlockCount(), is(2L));
      assertThrows(NoSuchElementException.class, reader::nextBlock);
    }
  }

  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_9,
      reason = "RandomData moved in Avro 1.9.x")
  @Test
  public void testRoundTripBiggerFile(@TempDir Path tmpDir) throws IOException {
    Schema schema = AvroUtil.api().parse(AvroTestResources.Recipe());
    File f = tmpDir.resolve("recipes.avro").toFile();

    // Write 500 records to the file using the RandomData instance.
    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      writer.create(schema, f);
      for (Object datum : new RandomData(schema, 5000, 0L)) {
        writer.append(datum);
      }
    }

    // Read all of the records from the file.
    long recordCount = 0;
    try (DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<>(f, new GenericDatumReader<>())) {
      for (GenericRecord r : dataFileReader) {
        recordCount++;
        if (recordCount == 100) {
          // Just check one of the values for a "random" data.
          assertThat(String.valueOf(r.get("source")), is("kidltikmqyxyeruhopv"));
        }
      }
    }

    assertThat(recordCount, is(5000L));
  }

  @Test
  public void testEmptyFile(@TempDir Path tmpDir) throws IOException {

    // An empty file can exist and contain no records (but can contain schema and metadata).
    File f = tmpDir.resolve("empty.avro").toFile();

    Schema schema = AvroUtil.api().parse(AvroTestResources.Recipe());
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(schema, GenericData.get()))) {
      // writer.setCodec(CodecFactory.snappyCodec());
      writer.setMeta("my.metadata", "This is my file");
      writer.create(schema, f);
    }

    assertThat(f, anExistingFile());
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(f, new GenericDatumReader<>(null, null, GenericData.get()))) {
      assertThat(reader.hasNext(), is(false));
      assertThat(reader.getMetaString("my.metadata"), is("This is my file"));
      assertThat(reader.getBlockCount(), is(0L));
      assertThat(reader.getSchema(), is(schema));
    }
  }
}
