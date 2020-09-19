package com.skraba.avro.enchiridion.resources

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

class BasicSpec extends AnyFunSpecLike with Matchers with BeforeAndAfterEach {

  describe("Prettify") {
    it("should convert a JSON string to a pretty version.") {
      val pretty = Json.prettyPrint(Json.parse(AvroTestResources.SimpleRecord))
      // print(pretty)
      pretty shouldBe AvroTestResources.SimpleRecord
    }
  }
}
