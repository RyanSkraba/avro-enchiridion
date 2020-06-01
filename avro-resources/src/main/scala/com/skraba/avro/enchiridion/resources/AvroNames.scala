package com.skraba.avro.enchiridion.resources

import play.api.libs.json.{JsObject, JsString, Json}

import scala.collection.immutable.ListMap
import scala.reflect.io.{File, Path}

/**
 * Generate valid and invalid schemas for checking named schema types.
 *
 * https://avro.apache.org/docs/current/spec.html#names
 */
object AvroNames {

  /**
   * Holder for the attributes that can be used in naming a schema type.  Missing optional attributes
   * are not included in the JSON snippet.
   *
   * @param tag       a tag to identify the configuration (unused in the schema).
   * @param namespace the namespace, if any.
   * @param name      the name to use, if any.
   * @param aliases   aliases for the name, if any.
   */
  case class NameCfg(tag: String,
                     namespace: Option[String] = None,
                     name: Option[String] = None,
                     aliases: Option[Seq[String]] = None) {

    def namespace(namespace: String): NameCfg = copy(namespace = Some(namespace))

    def namespace(namespace: String, name: String): NameCfg = copy(namespace = Some(namespace), name = Some(name))

    def name(name: String): NameCfg = copy(name = Some(name))

    def alias(alias: String): NameCfg = copy(aliases = Some(Seq(alias)))

    def aliases(aliases: Seq[String]): NameCfg = copy(aliases = Some(aliases))

    /**
     * @return A JSON object containing the namespace, name and aliases.
     */
    lazy val toJson: JsObject = JsObject(Seq(
      name.map("name" -> JsString(_)),
      namespace.map("namespace" -> JsString(_)),
      aliases.map("aliases" -> Json.arr(_).head.get)
    ).flatten)
  }

  /** Reusable constants for NameCfg. */
  object NameCfg {

    /** Valid combinations of name and namespaces. */
    val Valid: Seq[NameCfg] = Seq(
      NameCfg("Simple").name("Simple"),
      NameCfg("SimpleDigit").name("Simple9"),
      NameCfg("SimpleUnderscore").name("_Simple"),

      NameCfg("FullName").name("org.apache.avro.Simple"),
      NameCfg("FullNameDigit").name("org.apache.avro.Simple9"),
      NameCfg("FullNameUnderscore").name("org.apache.avro._Simple"),

      NameCfg("FullNamePackageDigit").name("org.apache.avro9.Simple"),
      NameCfg("FullNamePackageUnderscore").name("org.apache._avro.Simple"),

      NameCfg("NamespaceEmpty").namespace("", "Simple"),
      NameCfg("Namespace").namespace("org.apache.avro", "Simple"),
      NameCfg("NamespaceDigit").namespace("org.apache.avro9", "Simple"),
      NameCfg("NamespaceUnderscore").namespace("org.apache._avro", "Simple"),
      // Maybe?
      NameCfg("IgnoredNamespace").namespace("org..apache.äv rö.🚎..$!#!@%$", "org.apache.avro.Simple"),
    )

    /** Invalid combinations of name and namespaces. */
    val Invalid: Seq[NameCfg] = Seq(
      NameCfg("Anonymous"),
      NameCfg("EmptyName").name(""),
      NameCfg("AnonymousWithNamespace").namespace("org.apache.avro"),
      NameCfg("EmptyNameWithNamespace").namespace("org.apache.avro").name(""),

      NameCfg("NameAccent").name("Sïmplé"),
      NameCfg("NameEmoji").name("Sim🚎ple"),
      NameCfg("NameKebab").name("my-simple"),
      NameCfg("NameNumberStart").name("9Simple"),
      NameCfg("NamePercent").name("Sim%ple"),
      NameCfg("NameUnicodeDigit").name("Simple۵"),
      NameCfg("NameWhitespace").name("Sim ple"),

      NameCfg("NameStartDot").name(".Simple"),
      NameCfg("NameEndDot").name("Simple."),

      NameCfg("FullNameNumberStart").name("org.apache.avro.9Simple"),
      NameCfg("FullNameUnicodeDigit").name("org.apache.avro.Simple۵"),
      NameCfg("FullNameWhitespace").name("org.apache.avro.Sim ple"),
      NameCfg("FullNameAccent").name("org.apache.avro.Sïmplé"),
      NameCfg("FullNamePercent").name("org.apache.avro.Sim%ple"),
      NameCfg("FullNameKebab").name("org.apache.avro.my-simple"),
      NameCfg("FullNameEmoji").name("org.apache.avro.Sim🚎ple"),
      NameCfg("FullNameStartDot").name(".org.apache.avro.Simple"),
      NameCfg("FullNameEndDot").name("org.apache.avro.Simple."),
      NameCfg("FullNameDoubleDot").name("org.apache.avro..Simple"),
      NameCfg("FullNameEndDot").name("org.apache..avro.Simple"),

      NameCfg("FullNamePackageAccent").name("org.apache.âvrö.Simple"),
      NameCfg("FullNamePackageEmoji").name("org.apache.av🚎ro.Simple"),
      NameCfg("FullNamePackageKebab").name("org.apache.my-avro.Simple"),
      NameCfg("FullNamePackageNumberStart").name("org.apache.9avro.Simple"),
      NameCfg("FullNamePackagePercent").name("org.apache.av%ro.Simple"),
      NameCfg("FullNamePackageUnicodeDigit").name("org.apache.avro۵.Simple"),
      NameCfg("FullNamePackageWhitespace").name("org.apache.av ro.Simple"),

      NameCfg("NamespaceAccent").namespace("org.apache.âvrö", "Simple"),
      NameCfg("NamespaceEmoji").namespace("org.apache.av🚎ro", "Simple"),
      NameCfg("NamespaceKebab").namespace("org.apache.my-avro", "Simple"),
      NameCfg("NamespaceNumberStart").namespace("org.apache.9avro", "Simple"),
      NameCfg("NamespacePercent").namespace("org.apache.av%ro", "Simple"),
      NameCfg("NamespaceUnicodeDigit").namespace("org.apache.avro۵", "Simple"),
      NameCfg("NamespaceWhitespace").namespace("org.apache.av ro", "Simple"),

      NameCfg("NamespaceStartDot").namespace(".org.apache.avro", "Simple"),
      NameCfg("NamespaceEndDot").namespace("org.apache.avro.", "Simple"),
      NameCfg("NamespaceDoubleDot").namespace("org.apache..avro", "Simple")
    )

    /** Valid name without namespaces. */
    val ValidField: Seq[NameCfg] = Seq(
      // NameCfg("Id").name("id"), // included by default.
      NameCfg("IdDigit").name("id9"),
      NameCfg("IdUnderscore").name("_id")
    )

    /** Invalid names without namespaces. */
    val InvalidField: Seq[NameCfg] = Seq(
      NameCfg("FieldAccent").name("ïd"),
      NameCfg("FieldEmoji").name("i🚎d"),
      NameCfg("FieldKebab").name("my-id"),
      NameCfg("FieldNumberStart").name("9id"),
      NameCfg("FieldPercent").name("i%d"),
      NameCfg("FieldUnicodeDigit").name("id۵"),
      NameCfg("FieldWhitespace").name("i d"),

      NameCfg("FieldStartDot").name(".id"),
      NameCfg("FieldEndDot").name("id."),

      NameCfg("FieldFullName").name("org.apache.avro.Simple.id"))

    /** Valid aliases without namespaces. */
    val ValidAliases: Seq[NameCfg] = Seq(
      NameCfg("ZeroAliases").aliases(Seq()),
      NameCfg("OneAlias").alias("Alias"),
      NameCfg("SomeAlias").aliases(Seq("Alias", "Alias9", "_Alias")))

    /** Valid aliases with namespaces. */
    val ValidAliasesFullname = Seq(
      NameCfg("OneAliasFullName").alias("org.apache._avro9.Alias"),
      NameCfg("SomeAliasesFullName").aliases(Seq("org.apache._avro9.Alias", "org.apache._avro9.Alias9")))

    /** Invalid aliases with and without namespaces. */
    val InvalidAliases: Seq[NameCfg] = Seq(
      NameCfg("EmptyAlias").alias(""),
      NameCfg("AliasNameAccent").alias("Sïmplé"),
      NameCfg("AliasNameEmoji").alias("Sim🚎ple"),
      NameCfg("AliasNameKebab").alias("my-simple"),
      NameCfg("AliasNameNumberStart").alias("9Simple"),
      NameCfg("AliasNamePercent").alias("Sim%ple"),
      NameCfg("AliasNameUnicodeDigit").alias("Simple۵"),
      NameCfg("AliasNameWhitespace").alias("Sim ple"),

      NameCfg("AliasNameStartDot").alias(".Simple"),
      NameCfg("AliasNameEndDot").alias("Simple."),

      NameCfg("AliasFullNameNumberStart").alias("org.apache.avro.9Simple"),
      NameCfg("AliasFullNameUnicodeDigit").alias("org.apache.avro.Simple۵"),
      NameCfg("AliasFullNameWhitespace").alias("org.apache.avro.Sim ple"),
      NameCfg("AliasFullNameAccent").alias("org.apache.avro.Sïmplé"),
      NameCfg("AliasFullNamePercent").alias("org.apache.avro.Sim%ple"),
      NameCfg("AliasFullNameKebab").alias("org.apache.avro.my-simple"),
      NameCfg("AliasFullNameEmoji").alias("org.apache.avro.Sim🚎ple"),
      NameCfg("AliasFullNameStartDot").alias(".org.apache.avro.Simple"),
      NameCfg("AliasFullNameEndDot").alias("org.apache.avro.Simple."),
      NameCfg("AliasFullNameDoubleDot").alias("org.apache.avro..Simple"),
      NameCfg("AliasFullNameEndDot").alias("org.apache..avro.Simple"),

      NameCfg("AliasFullNamePackageAccent").alias("org.apache.âvrö.Simple"),
      NameCfg("AliasFullNamePackageEmoji").alias("org.apache.av🚎ro.Simple"),
      NameCfg("AliasFullNamePackageKebab").alias("org.apache.my-avro.Simple"),
      NameCfg("AliasFullNamePackageNumberStart").alias("org.apache.9avro.Simple"),
      NameCfg("AliasFullNamePackagePercent").alias("org.apache.av%ro.Simple"),
      NameCfg("AliasFullNamePackageUnicodeDigit").alias("org.apache.avro۵.Simple"),
      NameCfg("AliasFullNamePackageWhitespace").alias("org.apache.av ro.Simple")
    )
  }

  /**
   * Build a RECORD Schema as a JsObject.
   *
   * @param recordNameCfg The name attributes to apply to the record name.
   * @param fieldNameCfg  The name attributes to apply to the field name.
   * @return a one-column RECORD schema JSON using the given name configurations
   */
  def simpleRecord(recordNameCfg: NameCfg = NameCfg("").name("Simple"),
                   fieldNameCfg: NameCfg = NameCfg("").name("id")): JsObject = {
    val field = fieldNameCfg.toJson ++ Json.obj("type" -> "int")
    recordNameCfg.toJson ++ Json.obj("type" -> "record", "fields" -> Seq(field))
  }

  /**
   * Build an ENUM Schema as a JsObject.
   *
   * @param cfg    The name attributes to apply to the enum name.
   * @param symbol The symbol permitted for the enum.
   * @return a one-column ENUM schema JSON string using the given name configuration
   */
  def simpleEnum(cfg: NameCfg = NameCfg("").name("Simple"), symbol: String = "id"): JsObject = {
    cfg.toJson ++ Json.obj("type" -> "enum", "symbols" -> Seq(symbol))
  }

  /**
   * Build an FIXED Schema as a JsObject.
   *
   * @param cfg The name attributes to apply to the fixed name.
   * @return a one-column FIXED schema JSON string using the given name configuration
   */
  def simpleFixed(cfg: NameCfg = NameCfg("").name("Simple")): JsObject = {
    cfg.toJson ++ Json.obj("type" -> "fixed", "size" -> 1)
  }

  val ValidRecord: Map[String, JsObject] = ListMap(
    NameCfg.Valid.collect {
      case cfg => s"NameValidationRecord${cfg.tag}" -> simpleRecord(cfg)
    }: _*)

  val ValidRecordWithAliases: Map[String, JsObject] = ListMap(
    (NameCfg.ValidAliases ++ NameCfg.ValidAliasesFullname).collect {
      case cfg => s"NameValidationRecordWith${cfg.tag}" -> simpleRecord(cfg.name("Simple"))
    }: _*)

  val ValidField: Map[String, JsObject] = ListMap(
    NameCfg.ValidField.collect {
      case cfg => s"NameValidationField${cfg.tag}" -> simpleRecord(fieldNameCfg = cfg)
    }: _*)

  val ValidFieldWithAliases: Map[String, JsObject] = ListMap(
    NameCfg.ValidAliases.collect {
      case cfg => s"NameValidationFieldWith${cfg.tag}" -> simpleRecord(fieldNameCfg = cfg.name("id"))
    }: _*)

  val ValidEnum: Map[String, JsObject] = ListMap(
    NameCfg.Valid.collect {
      case cfg => s"NameValidationEnum${cfg.tag}" -> simpleEnum(cfg)
    }: _*)

  val ValidEnumSymbol: Map[String, JsObject] = ListMap(
    NameCfg.ValidField.collect {
      case cfg => s"NameValidationEnumSymbol${cfg.tag}" -> simpleEnum(symbol = cfg.name.get)
    }: _*)

  val ValidEnumWithAliases: Map[String, JsObject] = ListMap(
    (NameCfg.ValidAliases ++ NameCfg.ValidAliasesFullname).collect {
      case cfg => s"NameValidationEnumWith${cfg.tag}" -> simpleEnum(cfg.name("Simple"))
    }: _*)

  val ValidFixed: Map[String, JsObject] = ListMap(
    NameCfg.Valid.collect {
      case cfg => s"NameValidationFixed${cfg.tag}" -> simpleFixed(cfg)
    }: _*)

  val ValidFixedWithAliases: Map[String, JsObject] = ListMap(
    (NameCfg.ValidAliases ++ NameCfg.ValidAliasesFullname).collect {
      case cfg => s"NameValidationFixedWith${cfg.tag}" -> simpleFixed(cfg.name("Simple"))
    }: _*)

  val InvalidRecord: Map[String, JsObject] = ListMap(
    NameCfg.Invalid.collect {
      case cfg => s"NameValidationErrorRecord${cfg.tag}" -> simpleRecord(cfg)
    }: _*)

  val InvalidRecordWithAliases: Map[String, JsObject] = ListMap(
    NameCfg.InvalidAliases.collect {
      case cfg => s"NameValidationErrorRecordWith${cfg.tag}" -> simpleRecord(cfg.name("Simple"))
    }: _*)

  val InvalidField: Map[String, JsObject] = ListMap(
    NameCfg.InvalidField.collect {
      case cfg => s"NameValidationErrorField${cfg.tag}" -> simpleRecord(fieldNameCfg = cfg)
    }: _*)

  val InvalidFieldWithAliases: Map[String, JsObject] = ListMap(
    (NameCfg.ValidAliasesFullname ++ NameCfg.InvalidAliases).collect {
      case cfg => s"NameValidationErrorFieldWith${cfg.tag}" -> simpleRecord(fieldNameCfg = cfg.name("id"))
    }: _*)

  val InvalidEnum: Map[String, JsObject] = ListMap(
    NameCfg.Invalid.collect {
      case cfg => s"NameValidationErrorEnum${cfg.tag}" -> simpleEnum(cfg)
    }: _*)

  val InvalidEnumSymbol: Map[String, JsObject] = ListMap(
    NameCfg.InvalidField.collect {
      case cfg => s"NameValidationErrorEnumSymbol${cfg.tag}" -> simpleEnum(symbol = cfg.name.get)
    }: _*)

  val InvalidEnumWithAliases: Map[String, JsObject] = ListMap(
    NameCfg.InvalidAliases.collect {
      case cfg => s"NameValidationErrorEnumWith${cfg.tag}" -> simpleEnum(cfg.name("Simple"))
    }: _*)

  val InvalidFixed: Map[String, JsObject] = ListMap(
    NameCfg.Invalid.collect {
      case cfg => s"NameValidationErrorFixed${cfg.tag}" -> simpleFixed(cfg)
    }: _*)

  val InvalidFixedWithAliases: Map[String, JsObject] = ListMap(
    NameCfg.InvalidAliases.collect {
      case cfg => s"NameValidationErrorFixedWith${cfg.tag}" -> simpleFixed(cfg.name("Simple"))
    }: _*)

  val Valid: Map[String, JsObject] = (
    ValidRecord
      ++ ValidRecordWithAliases
      ++ ValidField
      ++ ValidFieldWithAliases
      ++ ValidEnum
      ++ ValidEnumSymbol
      ++ ValidEnumWithAliases
      ++ ValidFixed
      ++ ValidFixedWithAliases)

  val Invalid: Map[String, JsObject] = (
    InvalidRecord
      ++ InvalidRecordWithAliases
      ++ InvalidField
      ++ InvalidFieldWithAliases
      ++ InvalidEnum
      ++ InvalidEnumSymbol
      ++ InvalidEnumWithAliases
      ++ InvalidFixed
      ++ InvalidFixedWithAliases)

  def valids(): Array[String] = Valid.values.toArray.map(Json.stringify)

  def invalids(): Array[String] = Invalid.values.toArray.map(Json.stringify)

  /** Create the two files in the /tmp directory. */
  def main(args: Array[String]) {
    val dst: Path = sys.env.get("AVRO_ENCHIRIDION_REPO_DIR")
      .map(File(_).resolve("avro-resources/src/test/resources/"))
      .getOrElse(File("/tmp"))

    // val dst = s"${System.getProperty("user.home")}/working/github/avro/lang/java/avro/src/test/resources"
    dst.resolve("name-validation-good.txt").toFile.writeAll(
      Valid.map { kv => kv._1 + ":" + Json.stringify(kv._2) + "\n" }.toSeq: _*)
    dst.resolve("name-validation-bad.txt").toFile.writeAll(
      Invalid.map { kv => kv._1 + ":" + Json.stringify(kv._2) + "\n" }.toSeq: _*)
  }

}
