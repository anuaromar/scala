import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Date
import org.apache.spark.SparkContext


object DataProcessor {

  // Function to find passengers who have been on more than N flights together within a date range
  def flownTogether(atLeastNTimes: Int, fromDate: Date, toDate: Date, joinedDF: DataFrame,spark: SparkSession): DataFrame = {

    import spark.implicits._

    // Filter the joined data by the specified date range (from -> to)
    val filteredByDateDF = joinedDF
      .filter($"date" >= lit(fromDate) && $"date" <= lit(toDate))

    // Self-join the dataset on flightId to find pairs of passengers on the same flight
    val passengerPairsDF = filteredByDateDF.as("df1")
      .join(filteredByDateDF.as("df2"), $"df1.flightId" === $"df2.flightId" && $"df1.passengerId" < $"df2.passengerId")
      .select(
        $"df1.passengerId".alias("passenger1"),
        $"df2.passengerId".alias("passenger2"),
        $"df1.flightId",
        $"df1.date".alias("flightDate")
      )

    // Group by passenger pairs and aggregate by counting flights, and capturing the first/last flight dates
    val flightsTogetherDF = passengerPairsDF
      .groupBy("passenger1", "passenger2")
      .agg(
        count("flightId").alias("flightsTogether"),
        min("flightDate").alias("firstFlightDate"),
        max("flightDate").alias("lastFlightDate")
      )

    // Filter for passenger pairs who have flown together more than the specified number of times
    val frequentFlyerPairsDF = flightsTogetherDF
      .filter($"flightsTogether" > atLeastNTimes)
      .select(
        $"passenger1".alias("Passenger 1 ID"),
        $"passenger2".alias("Passenger 2 ID"),
        $"flightsTogether".alias("Number of flights together"),
        $"firstFlightDate".alias("From"),
        $"lastFlightDate".alias("To")
      )
      .orderBy(desc("Number of flights together"))

    frequentFlyerPairsDF
  }

  def exportToCSV(df: DataFrame, outputPath: String, coalesceToSingleFile: Boolean = true)(implicit spark: SparkSession): Unit = {
    val dfToWrite = if (coalesceToSingleFile) df.coalesce(1) else df
    val tempOutputPath = outputPath + "_temp"

    // Write the DataFrame to a temporary folder
    dfToWrite.write
      .option("header", "true")
      .csv(tempOutputPath)

    // Get the Hadoop filesystem using the SparkSession
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val srcPath = new Path(tempOutputPath)
    val dstPath = new Path(outputPath)

    // Move the part-00000.csv file to the desired output path
    val tempDir = fs.listStatus(srcPath).filter(_.getPath.getName.startsWith("part-")).head.getPath
    fs.rename(tempDir, dstPath)

    // Remove the temporary folder
    fs.delete(srcPath, true)
  }

  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Data Processor")
      .master("local[*]") // Use local mode for testing
      .getOrCreate()

    import spark.implicits._

    // Read passenger data from CSV
    val passengerDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/passengers.csv")

    // Read flight data from CSV
    val flightDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/flightData.csv")

    // Join the DataFrames on passengerId
    val joinedDF = passengerDF.join(flightDF, Seq("passengerId"), "inner") // inner join

    // Find the total number of flights for each month
    val flightCountsByMonth = joinedDF.withColumn("month", month(col("date")))
      .groupBy("month")
      .agg(count("flightId").alias("flight_count")).orderBy("month")
      .select(
        $"month".alias("Month"),
        $"flight_count".alias("Number of Flights"),
      )

    // Call the exportToCSV function to save flight counts to CSV
    val outputPath = "output/flightCountsByMonth.csv"
    exportToCSV(flightCountsByMonth, outputPath)
    flightCountsByMonth.show()

    // Find the names of the 100 most frequent flyers
    val frequentFlyers = joinedDF.groupBy("passengerId", "firstName", "lastName")
      .agg(count("flightId").alias("flight_count"))
      .orderBy(desc("flight_count")).limit(100)
      .select(
        $"passengerId".alias("Passenger ID"),
        $"flight_count".alias("Number of Flights"),
        $"firstName".alias("First Name"),
        $"lastName".alias("Last Name")
      )

    // Call the exportToCSV function to save frequent flyers to CSV
    val outputPath2 = "output/frequentFlyers.csv"
    exportToCSV(frequentFlyers, outputPath2)
    frequentFlyers.show()

    // Find the greatest number of countries a passenger has been in without being in the UK
    val flightsWithoutUK = joinedDF.filter(!$"from".contains("uk") && !$"to".contains("uk"))
      .select("passengerId", "flightId", "from", "to", "date")

    // Create window specification to track flight sequences by passenger
    val windowSpec = Window.partitionBy("passengerId").orderBy("date")

    // Track if the flight is consecutive in a sequence (for detecting sequences of flights without UK)
    val flightsWithLag = flightsWithoutUK.withColumn("prevCountry", lag("to", 1).over(windowSpec))

    // Add a column to detect when a new sequence starts
    val flightsWithSequence = flightsWithLag
      .withColumn("sequence",
        when($"prevCountry".isNull || $"from" =!= $"prevCountry", lit(1)).otherwise(lit(0))
      )

    // Use a cumulative sum to track sequence groups
    val flightsWithGroup = flightsWithSequence.withColumn("groupId", sum("sequence").over(windowSpec))

    // Group by passenger and groupId to find the longest sequence of unique countries without UK
    val longestSeq = flightsWithGroup
      .groupBy("passengerId", "groupId")
      .agg(countDistinct("to").alias("uniqueCountries")) // Count distinct countries in each sequence
      .groupBy("passengerId") // Group by passenger to find the max sequence
      .agg(max("uniqueCountries").alias("maxCountriesWithoutUK")) // Find the max sequence for each passenger
      .orderBy(desc("maxCountriesWithoutUK")) // Order by the longest sequence
      .select(
        $"passengerId".alias("Passenger ID"),
        $"maxCountriesWithoutUK".alias("Longest Run"),
      )

    // Call the exportToCSV function to save longest runs to CSV
    val outputPath3 = "output/longestRun.csv"
    exportToCSV(longestSeq, outputPath3)
    longestSeq.show()

    // Find the passengers who have been on more than 3 flights together
    val passengerPairsDF = joinedDF.as("df1")
      .join(joinedDF.as("df2"), $"df1.flightId" === $"df2.flightId" && $"df1.passengerId" < $"df2.passengerId")
      .select($"df1.passengerId".alias("passenger1"), $"df2.passengerId".alias("passenger2"), $"df1.flightId")

    // Group by passenger pairs and count the number of flights they have been on together
    val flightsTogetherDF = passengerPairsDF
      .groupBy("passenger1", "passenger2")
      .agg(count("flightId").alias("flightsTogether"))

    // Filter for passenger pairs who have been on more than 3 flights together
    val frequentFlyerPairsDF = flightsTogetherDF
      .filter($"flightsTogether" > 3)
      .orderBy(desc("flightsTogether"))
      .select(
        $"passenger1".alias("Passenger 1 ID"),
        $"passenger2".alias("Passenger 2 ID"),
        $"flightsTogether".alias("Number of flights together"),
      )

    // Call the exportToCSV function to save frequent flyer pairs to CSV
    val outputPath4 = "output/frequentFlyerPairs.csv"
    exportToCSV(frequentFlyerPairsDF, outputPath4)
    frequentFlyerPairsDF.show()


    // Define parameters
    val minFlights = 2
    val fromDate = Date.valueOf("2017-01-01") // start date in YYYY-MM-DD format
    val toDate = Date.valueOf("2017-12-31")   // end date in YYYY-MM-DD format
    // Call the function and get the result DataFrame
    val resultDF = flownTogether(minFlights, fromDate, toDate, joinedDF, spark)
    val outputPath5 = "output/flownTogether.csv"
    exportToCSV(frequentFlyerPairsDF, outputPath5)
    resultDF.show()

    // Stop the Spark session
    spark.stop()
  }
}
