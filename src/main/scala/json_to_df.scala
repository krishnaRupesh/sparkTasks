import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/**
 * In general spark.read.json  cant able to read multiple json objects in single file.
 * So we have to read it as text and have to pass it as a dataset.
 */
object json_to_df extends App{

  val spark = SparkSession.builder.appName("json_to_df").master("local")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

  val data = spark.read.text("src/main/resources/test.json")

  val required = data.select(
    get_json_object(col("value"),"$.payload.product").as("required"))

  /**
   * required.rdd.map(_.toString()) this converts the df to rdd. json reader of rdd is deprecated.
   * required.as[(String)] so we are using datasets.
   * spark.implicits._ Implicit methods available in Scala for converting
   * common Scala objects into `DataFrame`s.
   */
  //val json_data = spark.read.json(required.rdd.map(_.toString()))
  import spark.implicits._
  val json_data = spark.read.json(required.as[(String)])

  val output = json_data.select(col("productId"),explode(col("searchAdd")))

  val new_output = output.select(output.col("productId"),
    output.col("col.displayableSearchTerm").as("displayableSearchTerm"),
    output.col("col.rank").as("rank"))


  val w = Window.partitionBy("productId").orderBy("rank")

  val sortedDf = new_output.withColumn("sorted_by_rank", collect_list("displayableSearchTerm").over(w))

  val overall = sortedDf.groupBy(col("productId")).agg(max("sorted_by_rank").as("list_displayableSearchTerm"))

  //val overall = new_output.groupBy("productId").agg(collect_list("displayableSearchTerm").over(w))

  //val overall = new_output.groupBy("productId").agg(collect_list("displayableSearchTerm").as("list_displayableSearchTerm"))

  val arrayDFColumn = overall.select(
    overall("productId") +: (0 until 20).map(i => overall("list_displayableSearchTerm")(i).alias(s"list_displayableSearchTerm$i")): _*)
  arrayDFColumn.printSchema()
  arrayDFColumn.show(false)
}