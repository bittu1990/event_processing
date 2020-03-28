//Importing all required libraries

import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, from_json}

//S3 Buckets can be accessed by providing the AWS access key in Spark Configuration 
//val s3_bucket = "s3://dataeng-challenge/8uht6u8bh/events/*.*"
//val eventDF = spark.read.json(file_location)

//This is load the JSON file and all the invalid records will be stored in "_corrupt_record" system column
val file_location = "/FileStore/tables/sample_event.json"
val eventDF = spark.read.json(file_location)

//This will take nested JSON and flatten all the columns
val flattenedEventDF = eventDF.select($"id", $"created_at", $"event_name", $"ip", $"metadata.*", $"user_email").filter("id is not null")

//Create view from Event DataFrame
flattenedEventDF.createOrReplaceTempView("vw_event_details")
val eventDetailsDF = spark.sql("select * from vw_event_details")

//Handling corrupt event record

//Filter the "_corrupt_record" column
val corruptedEventDF = eventDF.filter($"_corrupt_record".isNotNull)
corruptedEventDF.cache()

//Create veiw for corrupt records
corruptedEventDF.createOrReplaceTempView("vw_corrupt_event_details")

//Start position of any valid event will be from key - ID
val startingPos = """ {"id":"""

//End position of any valid event will be closing  curly braces "}}" 
val endingPos = """}}')))"""

//SQL to select the required string from the corrupt event entries
val sqlString = "select TRIM(SUBSTR(_corrupt_record, INSTR(_corrupt_record, '"+startingPos+"') ,"+" INSTR(_corrupt_record, '"+endingPos+" AS corrupt_record from   vw_corrupt_event_details"

val rectifiedRecordDF = spark.sql(sqlString)

//Flattening the corrupt event DataFrame to parse nested JSON
val tempRectifiedDF = rectifiedRecordDF.map(_.getString(0))
val flatRectifiedRecordDF = spark.read.json(tempRectifiedDF).select($"id", $"created_at", $"event_name", $"ip", $"metadata.*", $"user_email")

//Adding missing columns to event and corrupt event DataFrame
//Columns in event DF
val col_event = flattenedEventDF.columns.toSet

//Columns in corrupt DF
val col_rectified_event = flatRectifiedRecordDF.columns.toSet

//ALl columns
val all_cols = col_event ++ col_rectified_event
val cols_string = all_cols.mkString(" , ")

//Function to add missing columns
def addAllColumns(df: DataFrame, expectedColumnsInput: Set[String]) : DataFrame = {
    expectedColumnsInput.foldLeft(df) {
        (df,column) => {
            if(df.columns.contains(column) == false) {
                df.withColumn(column,lit(null).cast(StringType))
            }
            else (df)
        }
    }
}

//Generating the final event DataFrame
//Adding missing columns to event DF
val eventAllColsDF = addAllColumns(flattenedEventDF, all_cols)
eventAllColsDF.cache()

//Adding missing columns to corrupt event DF
val rectifiedAllColsDF = addAllColumns(flatRectifiedRecordDF, all_cols)
rectifiedAllColsDF.cache()

//Creating views for event and corrupt event DF
eventAllColsDF.createOrReplaceTempView("vw_event_all_cols")
rectifiedAllColsDF.createOrReplaceTempView("vw_rectified_event_all_cols")

//Final event DF
val finalEventDF = spark.sql("select "+ cols_string + " from vw_event_all_cols  union select " + cols_string + " from vw_rectified_event_all_cols")

//Saving into directory for further processing
finalEventDF.write.mode("overwrite").option("header", "true").format("csv").save("/FileStore/tables/finalEventDetails")
