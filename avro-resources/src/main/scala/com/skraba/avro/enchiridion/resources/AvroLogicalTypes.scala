package com.skraba.avro.enchiridion.resources

import play.api.libs.json.{JsObject, Json}

/** Generate valid and invalid schemas for checking schema logical types.
  *
  * @see https://avro.apache.org/docs/current/spec.html#Logical+Types
  */
object AvroLogicalTypes {

  lazy val TimestampMillis: JsObject =
    Json.obj("type" -> "long", "logicalType" -> "timestamp-millis")
  lazy val TimestampMicros: JsObject =
    Json.obj("type" -> "long", "logicalType" -> "timestamp-micros")
  lazy val Date: JsObject = Json.obj("type" -> "int", "logicalType" -> "date")
  lazy val TimeMillis: JsObject =
    Json.obj("type" -> "int", "logicalType" -> "time-millis")
  lazy val TimeMicros: JsObject =
    Json.obj("type" -> "long", "logicalType" -> "time-micros")

  /** Added in 1.10.0 */
  lazy val LocalTimestampMillis: JsObject =
    Json.obj("type" -> "long", "logicalType" -> "local-timestamp-millis")

  /** Added in 1.10.0 */
  lazy val LocalTimestampMicros: JsObject =
    Json.obj("type" -> "long", "logicalType" -> "local-timestamp-micros")

  /** In a field, the logicalType attribute needs to be nested. None of these fields have logical types.
    *
    * ==WRONG==
    *
    * {{{
    * ...
    * { "name": "datetime_ms", "type": "long", "logicalType": "timestamp-millis" }
    * ...
    * }}}
    *
    * ==RIGHT==
    *
    * {{{
    * ...
    * { "name": "datetime_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}}
    * ...
    * }}}
    */
  lazy val DateLogicalTypeRecordInvalid: JsObject = Json.obj(
    "type" -> "record",
    "name" -> "DateLogicalTypeRecordInvalid",
    "namespace" -> "com.skraba.avro.enchiridion.simple",
    "fields" -> Json.arr(
      Json.obj("name" -> "name", "type" -> "string"),
      Json.obj("name" -> "datetime_ms") ++ TimestampMillis,
      Json.obj("name" -> "datetime_us") ++ TimestampMicros,
      Json.obj("name" -> "local_datetime_ms") ++ LocalTimestampMillis,
      Json.obj("name" -> "local_datetime_us") ++ LocalTimestampMicros,
      Json.obj("name" -> "date") ++ Date,
      Json.obj("name" -> "time_ms") ++ TimeMillis,
      Json.obj("name" -> "time_us") ++ TimeMicros
    )
  )

  lazy val DateLogicalTypeRecord: JsObject = Json.obj(
    "type" -> "record",
    "name" -> "DateLogicalTypeRecord",
    "namespace" -> "com.skraba.avro.enchiridion.simple",
    "fields" -> Json.arr(
      Json.obj("name" -> "name", "type" -> "string"),
      Json.obj("name" -> "datetime_ms", "type" -> TimestampMillis),
      Json.obj("name" -> "datetime_us", "type" -> TimestampMicros),
      Json.obj("name" -> "local_datetime_ms", "type" -> LocalTimestampMillis),
      Json.obj("name" -> "local_datetime_us", "type" -> LocalTimestampMicros),
      Json.obj("name" -> "date", "type" -> Date),
      Json.obj("name" -> "time_ms", "type" -> TimeMillis),
      Json.obj("name" -> "time_us", "type" -> TimeMicros)
    )
  )

  lazy val DateLogicalTypeOptionalRecord: JsObject = Json.obj(
    "type" -> "record",
    "name" -> "DateLogicalTypeOptionalRecord",
    "namespace" -> "com.skraba.avro.enchiridion.simple",
    "fields" -> Json.arr(
      Json.obj("name" -> "name", "type" -> "string"),
      Json.obj(
        "name" -> "datetime_ms",
        "type" -> Json.arr("null", TimestampMillis)
      ),
      Json.obj(
        "name" -> "datetime_us",
        "type" -> Json.arr("null", TimestampMicros)
      ),
      Json.obj(
        "name" -> "local_datetime_ms",
        "type" -> Json.arr("null", LocalTimestampMillis)
      ),
      Json.obj(
        "name" -> "local_datetime_us",
        "type" -> Json.arr("null", LocalTimestampMicros)
      ),
      Json.obj("name" -> "date", "type" -> Json.arr("null", Date)),
      Json.obj("name" -> "time_ms", "type" -> Json.arr("null", TimeMillis)),
      Json.obj("name" -> "time_us", "type" -> Json.arr("null", TimeMicros))
    )
  )

  lazy val DecimalBytes52: JsObject =
    Json.obj(
      "type" -> "bytes",
      "logicalType" -> "decimal",
      "precision" -> 5,
      "scale" -> 2
    )

  lazy val DecimalLogicalTypeRecord: JsObject = Json.obj(
    "type" -> "record",
    "name" -> "DecimalLogicalTypeRecord",
    "namespace" -> "com.skraba.avro.enchiridion.simple",
    "fields" -> Json.arr(
      Json.obj("name" -> "name", "type" -> "string"),
      Json.obj("name" -> "bytes52", "type" -> DecimalBytes52)
    )
  )

  def decimalBytes(precision: Object, scale: Object): String =
    s"""{
       |  "type":"bytes",
       |  "logicalType":"decimal",
       |  "precision":$precision,
       |  "scale":$scale
       |}""".stripMargin

  def decimalFixed(precision: Object, scale: Object, size: Object): String =
    s"""{
       |  "fixed":"fixed",
       |  "name":"fixedP${precision}S$scale",
       |  "size":$size",
       |  "logicalType":"decimal",
       |  "precision":$precision,
       |  "scale":$scale
       |}""".stripMargin
}
