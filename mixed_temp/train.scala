import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object trainPredictObj
{
	def main(args: Array[String]): Unit =
	{
		val sc = new SparkContext(new SparkConf().setAppName("GraphData"))

		val baseRDD = sc.textFile("newtrain.csv")
		val testRDD = sc.textFile("newtest.csv")

		val lpRDD = baseRDD.map
		{
			line =>
			val allFields = line.split(',')
			val label = allFields(0).toInt
			val features = allFields.slice(3,allFields.length).map(_.toDouble)
			LabeledPoint(label, Vectors.dense(features))
		}

		val testLpRDD = testRDD.map
		{
			line =>
			val allFields = line.split(',')
			val features = allFields.slice(2,allFields.length).map(_.toDouble)
			LabeledPoint(0, Vectors.dense(features))
		}

		val testLabels = testRDD.map
		{
			line =>
			val allFields = line.split(',')
			(allFields(0), allFields(1))
		}

		val Array(trainData, cvData) = lpRDD.randomSplit(Array(0.7,0.3))

		lpRDD.cache()
		trainData.cache()
		cvData.cache()
		testLpRDD.cache()

		getBestLRResults(trainData, cvData)

		val lr = new LogisticRegressionWithLBFGS()
		lr.optimizer.setNumIterations(100).setRegParam(0.01)
		
		var model = lr.run(trainData)
		val predictionsAndLabels = cvData.map(lp => (model.predict(lp.features), lp.label))
		println(new BinaryClassificationMetrics(predictionsAndLabels).areaUnderROC())

		model = lr.run(lpRDD)
		model.clearThreshold()
		val predictions = testLpRDD.map(lp => model.predict(lp.features))
		testLabels.zip(predictions).map{case(lblTuples, predictProba) => lblTuples._1 + "," + lblTuples._2 + "," + "%.7f".format(predictProba)}.saveAsTextFile("solution")
	}

	def getBestLRResults(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit =
	{
		val evaluations = for(
				numIterations <- Array(100,200);
				regParam <- Array(0.001,0.01,0.1,1.0)
		)
		yield
		{
			val lr = new LogisticRegressionWithLBFGS()
			lr.optimizer.setNumIterations(numIterations).setRegParam(regParam)
			
			val model = lr.run(trainData)
			val predictionsAndLabels = cvData.map(lp => (model.predict(lp.features), lp.label))
			(numIterations, regParam, new BinaryClassificationMetrics(predictionsAndLabels).areaUnderROC())
		}
		evaluations.sortBy(_._3).reverse.foreach(println)
	}
}