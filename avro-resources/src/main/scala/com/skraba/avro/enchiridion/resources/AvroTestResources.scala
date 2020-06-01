package com.skraba.avro.enchiridion.resources

/**
 * Reusable resources for Avro tests.
 */
object AvroTestResources {

  val SimpleRecord: String =
    """
      |{
      |  "type": "record",
      |  "name": "SimpleRecord",
      |  "namespace": "com.skraba.avro.echiridion.resources",
      |  "doc":"Simple two column record",
      |  "fields": [
      |    {"name": "id", "type": "long"},
      |    {"name": "name", "type": "string"}
      |  ]
      |}""".stripMargin

  val Avro1965: String =
    """{
      |  "type" : "record",
      |  "name" : "a",
      |  "namespace" : "default",
      |  "fields" : [ {
      |    "name" : "b",
      |    "type" : {
      |      "type" : "record",
      |      "name" : "b",
      |      "namespace" : "",
      |      "fields" : [ {
      |        "name" : "c",
      |        "type" : {
      |          "type" : "record",
      |          "name" : "c",
      |          "fields" : [ {
      |            "name" : "d",
      |            "type" : "int"
      |          } ]
      |        }
      |      } ]
      |    }
      |  } ]
      |}""".stripMargin

}
