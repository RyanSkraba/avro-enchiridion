package com.skraba.avro.enchiridion.resources

import play.api.libs.json.Json.{arr, obj}
import play.api.libs.json.JsObject

/** Generate valid and invalid schemas for checking schema logical types.
  *
  * @see https://avro.apache.org/docs/current/spec.html#Logical+Types
  */
object AvroLogicalTypes {

  lazy val TimestampMillis: JsObject =
    obj("type" -> "long", "logicalType" -> "timestamp-millis")
  lazy val TimestampMicros: JsObject =
    obj("type" -> "long", "logicalType" -> "timestamp-micros")
  lazy val Date: JsObject = obj("type" -> "int", "logicalType" -> "date")
  lazy val TimeMillis: JsObject =
    obj("type" -> "int", "logicalType" -> "time-millis")
  lazy val TimeMicros: JsObject =
    obj("type" -> "long", "logicalType" -> "time-micros")

  /** Added in 1.10.0 */
  lazy val LocalTimestampMillis: JsObject =
    obj("type" -> "long", "logicalType" -> "local-timestamp-millis")

  /** Added in 1.10.0 */
  lazy val LocalTimestampMicros: JsObject =
    obj("type" -> "long", "logicalType" -> "local-timestamp-micros")

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
  lazy val DateLogicalTypeRecordInvalid: JsObject = obj(
    "type" -> "record",
    "name" -> "DateLogicalTypeRecordInvalid",
    "namespace" -> "com.skraba.avro.enchiridion.simple",
    "fields" -> arr(
      obj("name" -> "name", "type" -> "string"),
      obj("name" -> "datetime_ms") ++ TimestampMillis,
      obj("name" -> "datetime_us") ++ TimestampMicros,
      obj("name" -> "local_datetime_ms") ++ LocalTimestampMillis,
      obj("name" -> "local_datetime_us") ++ LocalTimestampMicros,
      obj("name" -> "date") ++ Date,
      obj("name" -> "time_ms") ++ TimeMillis,
      obj("name" -> "time_us") ++ TimeMicros
    )
  )

  lazy val DateLogicalTypeRecord: JsObject = obj(
    "type" -> "record",
    "name" -> "DateLogicalTypeRecord",
    "namespace" -> "com.skraba.avro.enchiridion.simple",
    "fields" -> arr(
      obj("name" -> "name", "type" -> "string"),
      obj("name" -> "datetime_ms", "type" -> TimestampMillis),
      obj("name" -> "datetime_us", "type" -> TimestampMicros),
      obj("name" -> "local_datetime_ms", "type" -> LocalTimestampMillis),
      obj("name" -> "local_datetime_us", "type" -> LocalTimestampMicros),
      obj("name" -> "date", "type" -> Date),
      obj("name" -> "time_ms", "type" -> TimeMillis),
      obj("name" -> "time_us", "type" -> TimeMicros)
    )
  )

  lazy val DateLogicalTypeOptionalRecord: JsObject = obj(
    "type" -> "record",
    "name" -> "DateLogicalTypeOptionalRecord",
    "namespace" -> "com.skraba.avro.enchiridion.simple",
    "fields" -> arr(
      obj("name" -> "name", "type" -> "string"),
      obj(
        "name" -> "datetime_ms",
        "type" -> arr("null", TimestampMillis)
      ),
      obj(
        "name" -> "datetime_us",
        "type" -> arr("null", TimestampMicros)
      ),
      obj(
        "name" -> "local_datetime_ms",
        "type" -> arr("null", LocalTimestampMillis)
      ),
      obj(
        "name" -> "local_datetime_us",
        "type" -> arr("null", LocalTimestampMicros)
      ),
      obj("name" -> "date", "type" -> arr("null", Date)),
      obj("name" -> "time_ms", "type" -> arr("null", TimeMillis)),
      obj("name" -> "time_us", "type" -> arr("null", TimeMicros))
    )
  )

  lazy val DecimalBytes52: JsObject =
    obj(
      "type" -> "bytes",
      "logicalType" -> "decimal",
      "precision" -> 5,
      "scale" -> 2
    )

  lazy val DecimalLogicalTypeRecord: JsObject = obj(
    "type" -> "record",
    "name" -> "DecimalLogicalTypeRecord",
    "namespace" -> "com.skraba.avro.enchiridion.simple",
    "fields" -> arr(
      obj("name" -> "name", "type" -> "string"),
      obj("name" -> "bytes52", "type" -> DecimalBytes52)
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
