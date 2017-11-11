import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val diabetesFile = "C:/dataForScalaProjects/pima-indians-diabetes.csv"
    val outputFile = "C:/dataForScalaProjects/predictions.csv"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-read-csv")
      .master("local[*]")
      .getOrCreate()

    val dataSet = new DataSetReader(sparkSession, diabetesFile)

    val DFAssembler = new VectorAssembler().
      setInputCols(Array(
        "Pregnancy", "Glucose", "ArterialPressure", "ThicknessOfTC",
        "Insulin", "BodyMassIndex", "Heredity", "Age")).
      setOutputCol("features")

    val features = DFAssembler.transform(dataSet.getDataFrame())

    val labeledTransformer = new StringIndexer().setInputCol("Diabetes").setOutputCol("label")
    val labeledFeatures = labeledTransformer.fit(features).transform(features)

    val splits = labeledFeatures.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0) //60%
    val testData = splits(1) //40%

    val logisticRegression = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.03)
      .setElasticNetParam(0.8)

    //Train Model
    val model = logisticRegression.fit(trainingData)

    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    //Make predictions on test data
    val predictions: DataFrame = model.transform(testData)
    //predictions.show(200)

    //Evaluate the precision and recall
    val countProve = predictions.where("label == prediction").count().toDouble
    val count = predictions.count()

    println(s"Count of true predictions: $countProve Total Count: $count")
    println("Probability" + countProve / count)
  }
}
