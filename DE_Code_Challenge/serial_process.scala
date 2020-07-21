//spark-shell --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" -i games_csv_reader.scala

import java.io.File
import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,ArrayType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import java.nio.file._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

// establish data model for end result
//
// spark.sql("""drop table bike_info""");
// spark.sql("""
//   CREATE TABLE `bike_info`(
//    `serial_num` string,
//    `model` string,
//    `model_year` string,
//    `model_month` string,
//    `manufactured_year` string,
//    `factory_name` string,
//    `bike_version` string,
//    `serial_num_6` string,
//    `recieved_date` string
//    )
//   STORED AS ORC tblproperties ("orc.compress" = "SNAPPY")
//   """);

//get files in directory by extentions
def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
        extensions.exists(file.getName.endsWith(_))
    }
}

//move file function
def moveFile(fileName: File, folderName: File) = {
    Files.move(fileName.toPath(), folderName.toPath(), StandardCopyOption.REPLACE_EXISTING);
}

//pull all csvs from the serial_drive folder
val files = getListOfFiles(new File("./serial_drive"), List("csv"));

//establish schema for decodes
val decodeSchema = StructType(Array(
  StructField("code", StringType, true),
  StructField("map", StringType, true)
  )
)

//load decode CSVs
val df_model_decode = spark.read.format("csv").option("header", "false").schema(decodeSchema).load("./decode/model_decode.csv");
val df_month_decode = spark.read.format("csv").option("header", "false").schema(decodeSchema).load("./decode/month_decode.csv");
val df_year_decode = spark.read.format("csv").option("header", "false").schema(decodeSchema).load("./decode/year_decode.csv");
val df_factory_decode = spark.read.format("csv").option("header", "false").schema(decodeSchema).load("./decode/factory_decode.csv");

//establish schema for files loaded
val customSchema = StructType(Array(
  StructField("serial_num", StringType, true),
  StructField("file_name", StringType, true)
  )
)

//create empty dataframe
var df_csv_all = spark.createDataFrame(sc.emptyRDD[Row], customSchema);

//go through all files and load them
println("Loading Data from CSVs... Starting");
for (file <- files){
  val df_csv = spark.read.format("csv").option("header", "false").load(file.toString()).
    //add file name to be processed for load date
    withColumn("file_name", lit(file.getName().toLowerCase().replace(".csv", "")));

  df_csv_all = df_csv_all.union(df_csv);
}
println("Loading Data from CSVs... Complete");

//remove andy duplicates and take oldest recieved searial
df_csv_all = df_csv_all.
  withColumn("file_name_date", substring($"file_name", -15, 15)).
  withColumn("ts", to_timestamp($"file_name_date", "yyyyMMdd_HHmmss")).
  groupBy($"serial_num").
  agg(min("ts").alias("recieved_date"));

//seperate the serial_num into its peices
val df_pieces = df_csv_all.
  withColumn("model_piece", substring($"serial_num", 1, 1)).
  withColumn("year_piece", substring($"serial_num", 2, 1)).
  withColumn("month_piece", substring($"serial_num", 3, 1)).
  withColumn("manufactured_year", substring($"serial_num", 4, 2)).
  withColumn("factory_piece", substring($"serial_num", 6, 1)).
  withColumn("bike_version", substring($"serial_num", 7, 1)).
  withColumn("serial_num_6", substring($"serial_num", 8, 6));

//join to decoding tables
val df_pieces_model = df_pieces.join(df_model_decode, df_pieces.col("model_piece") === df_model_decode.col("code"), "left_outer").
  withColumnRenamed("map", "model");

val df_pieces_month = df_pieces_model.join(df_month_decode, df_pieces_model.col("month_piece") === df_month_decode.col("code"), "left_outer").
  withColumnRenamed("map", "model_month");

val df_pieces_year = df_pieces_month.join(df_year_decode, df_pieces_month.col("year_piece") === df_year_decode.col("code"), "left_outer").
  withColumnRenamed("map", "model_year");

val df_pieces_factory = df_pieces_year.join(df_factory_decode, df_pieces_year.col("factory_piece") === df_factory_decode.col("code"), "left_outer").
  withColumnRenamed("map", "factory_name");

//get end result and fill any nulls with "Unknown"
val df_output = df_pieces_factory.select("serial_num", "model", "model_year", "model_month", "manufactured_year", "factory_name", "bike_version", "serial_num_6", "recieved_date").na.fill("Unknown");

print("Rows pulled from CSVs: " + df_output.count());

val df_current = spark.sql("select * from bike_info");

print("Rows already in table: " + df_current.count());

//join to only remove rows that exist already in the db
val df_output_new = df_output.join(df_current, Seq("serial_num"), "left_anti");

print("Rows that didnt already exist: " + df_output_new.count());

//loading a temp table
df_output_new.createOrReplaceTempView("df_output_new");


println("Writing table bike_info ... starting");

spark.sql("""
  insert into bike_info
  select * from df_output_new
""");

println("Writing table bike_info ... complete");

//verify new row count
val df_verify = spark.sql("select * from bike_info");

print("Rows in table now: " + df_verify.count());

//move files from working to output
for (file <- files){
  val outputdir = new File("./serial_drive_completed/" + file.getName());
  moveFile(file, outputdir);
}

//merge csv into single output
def merge(srcPath: String, dstPath: String): Unit =  {
   new File(dstPath).delete();
   val hadoopConfig = new Configuration();
   val hdfs = FileSystem.get(hadoopConfig);
   FileUtil.copyMerge(hdfs, new org.apache.hadoop.fs.Path(srcPath), hdfs, new org.apache.hadoop.fs.Path(dstPath), true, hadoopConfig, null);
   // the "true" setting deletes the source files once they are merged into the new output
}

//output to verify
val outputfile = "./output/";
var filename = "bike_info.csv";
var outputFileName = outputfile + "/temp_" + filename ;
var mergedFileName = outputfile + "/merged_" + filename;
var mergeFindGlob  = outputFileName;

//writing file to csv for verification
df_verify.
  repartition(1).
  write.mode("overwrite").
  format("csv").
  option("header", true).
  option("delimiter", ",").
  save(outputFileName);

merge(mergeFindGlob, mergedFileName);



System.exit(0);
