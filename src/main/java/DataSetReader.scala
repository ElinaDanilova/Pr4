import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataSetReader(sparkSession: SparkSession, input: String) {
  var dataSetSchema = StructType(Array(
    StructField("Pregnancy", DoubleType, true),
    StructField("Glucose", DoubleType, true),
    StructField("ArterialPressure", DoubleType, true),
    StructField("ThicknessOfTC", DoubleType, true),
    StructField("Insulin", DoubleType, true),
    StructField("BodyMassIndex", DoubleType, true),
    StructField("Heredity", DoubleType, true),
    StructField("Age", DoubleType, true),
    StructField("Diabetes", DoubleType, true)))

  //Read CSV file to DF and define scheme on the fly
  private val diabetsDataFrame: DataFrame = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .schema(dataSetSchema)
    .csv(input)

  diabetsDataFrame.createOrReplaceTempView("diabetesTable")

  //Get all data without Diabetes column
  def getDataFrame(): DataFrame = {
    diabetsDataFrame
  }

  //Get all data without Diabetes column
  def getAllDataWithoutLastColumn(): DataFrame = {
    sparkSession.sql(
      " SELECT Pregnancy, Glucose, ArterialPressure, ThicknessOfTC, Insulin, BodyMassIndex, Heredity, Age" +
        " FROM diabetesTable")
  }

  //Get Diabetes column
  def getDataFromLastColumn(): DataFrame = {
    sparkSession.sql(
      " SELECT Diabetes" +
        " FROM diabetesTable")
  }

}
