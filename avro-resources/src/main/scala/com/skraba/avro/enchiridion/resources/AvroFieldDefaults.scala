package com.skraba.avro.enchiridion.resources

/**
 * Resources for testing default values in Avro schemas.
 */
object AvroFieldDefaults {

  case class FieldDefaultCfg(tag: String, fieldType: String, fieldDefault: Any)

  def simple(cfg: FieldDefaultCfg): String = {
    s"""{ "type":"record","name":"FieldDefault${cfg.tag}",
       |  "fields": [
       |    "name": "a1",
       |    "type": ${cfg.fieldType},
       |    "default": ${cfg.fieldDefault}
       |  ]
       |}""".stripMargin
  }

  val ValidFieldDefaults: Seq[FieldDefaultCfg] = Seq(
    FieldDefaultCfg("IntZero", """"int"""", "0"),
    FieldDefaultCfg("IntMax", """"int"""", Int.MaxValue),
    FieldDefaultCfg("IntMin", """"int"""", Int.MinValue),
    FieldDefaultCfg("LongZero", """"long"""", "0"),
    FieldDefaultCfg("LongMax", """"long"""", Long.MaxValue),
    FieldDefaultCfg("LongMin", """"long"""", Long.MinValue),
  )

  val InvalidFieldDefaults: Seq[FieldDefaultCfg] = Seq(
    FieldDefaultCfg("IntNull", """"int"""", "null"),
    FieldDefaultCfg("IntString", """"int"""", """"0""""),
    FieldDefaultCfg("IntOverflow", """"long"""", Long.MaxValue.toString),
    FieldDefaultCfg("LongNull", """"long"""", "null"),
    FieldDefaultCfg("LongString", """"long"""", """"0""""),
  )

  /** A record with a partially specified default.  The fields missing from the default also have defaults. */
  val Avro2844: String =
    """{
      |  "name" : "Zoo",
      |  "type" : "record",
      |  "fields" : [ {
      |    "name" : "pet",
      |    "type" : {
      |      "name" : "Pet",
      |      "type" : "record",
      |      "fields" : [ {
      |        "name" : "name",
      |        "type" : "string",
      |        "default" : ""
      |      }, {
      |        "name" : "weight",
      |        "type" : "long",
      |        "default" : 0
      |      } ]
      |    },
      |    "default" : { }
      |  } ]
      |}""".stripMargin
}
