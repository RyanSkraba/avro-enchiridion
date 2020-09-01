package com.skraba.avro.enchiridion.core.schema;

import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.resources.AvroTestResources;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Unit tests for fingerprinting a schema. */
public class FingerprintTest {

  @Test
  public void testBasic() throws NoSuchAlgorithmException {
    Schema schema = AvroUtil.api().parse(AvroTestResources.SimpleRecord());

    // Simplest fingerprint.
    long fp = SchemaNormalization.parsingFingerprint64(schema);
    assertThat(fp, is(-6444834972961693627L));
    assertThat(Long.toHexString(fp), is("a68f58bd009aa045"));

    // Explicitly asking for the Avro fingerprint.
    byte[] fpArray = SchemaNormalization.parsingFingerprint("CRC-64-AVRO", schema);

    // Convert the simpler method to a byte array.
    byte[] result = new byte[8];
    for (int i = 7; i >= 0; i--) {
      result[7 - i] = (byte) (fp & 0xFF); // With the right endiness.
      fp >>= 8;
    }

    assertThat(fpArray, is(result));
  }
}
