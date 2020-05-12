import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.feature.StandardScaler

object clusterObj
{
	def main(args: Array[String]): Unit =
	{
		val sc = new SparkContext(new SparkConf().setAppName("Clustering_Scala"))
		val rawData = sc.textFile("file:///C:/Users/dpatn/Desktop/Win_Share/projects/kdd_clustering/kddcup.data")

		//rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
		
		val labelsAndData = rawData.map
		{
			line =>
			val buffer = line.split(',').toBuffer
			buffer.remove(1, 3)
			val label = buffer.remove(buffer.length-1)
			val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
			(label,vector)
		}
		/*
		val labelsAndData = rawData.map
		{
			line =>
			val allFields = line.split(',')
			(allFields(0), Vectors.dense(allFields.slice(1,allFields.length).map(_.toDouble)))
		}
		*/
		val data = labelsAndData.values.cache()
		val keys = labelsAndData.keys
		val scaledData = new StandardScaler(withMean = false, withStd = true).fit(data).transform(data)
		val labelsAndScaledData = keys.zip(scaledData).cache()

		val kMeans = new KMeans()
		val model = kMeans.run(labelsAndScaledData.values)

		val labelsAndClusters = labelsAndScaledData.mapValues(model.predict)
		//println(labelsAndClusters.take(1)(0))
		val clustersAndLabels = labelsAndClusters.map(_.swap)
		//println(clustersAndLabels.take(1)(0))
		val labelsInClusters = clustersAndLabels.groupByKey().values
		//println(labelsInClusters.take(1)(0))

		val labelCounts = labelsInClusters.map(_.groupBy(l => l).map(_._2.size))
		val n = labelsAndScaledData.count()

		labelCounts.map(m => m.sum * entropy(m)).sum / n
		println(labelCounts.take(1)(0))

		// get threshold
		val distances = normalizedData.map(datum => distToCentroid(datum, model))
		val threshold = distances.top(100).last

		val model = ...
		val originalAndData = ...
		val anomalies = originalAndData.filter
		{
			case (original, datum) =>
			val normalized = normalizeFunction(datum)
			distToCentroid(normalized, model) > threshold
		}.keys

		
		def entropy(counts: Iterable[Int]) = 
		{
			val values = counts.filter(_ > 0)
			val n: Double = values.sum
			values.map
			{
				v =>
				val p = v / n
				-p * math.log(p)
			}.sum
		}

		//val scaledData = fData.map(lp => LabeledPoint(lp.label,scaler.transform(lp.features)))

		// Investigate data
/*
		val kmeans = new KMeans()
		val model = kmeans.run(data)
		model.clusterCenters.foreach(println)

		val clusterLabelCount = labelsAndData.map
		{
			case(lbl, datum) =>
			(model.predict(datum),lbl)
		}.countByValue

		clusterLabelCount.toSeq.sorted.foreach
		{
			case((cluster,label),count) =>
			println(f"$cluster%1s$label%18s$count%8s")
		}


		def distance(centroid: Vector, datum: Vector) =
		{
			math.sqrt(centroid.toArray.zip(datum.toArray).map(p => p._1 - p._2).map(d => d * d).sum)
		}

		def distToCentroid(datum: Vector, model: KMeansModel) =
		{
			val cluster = model.predict(datum)
			val centroid = model.clusterCenters(cluster)
			distance(centroid, datum)
		}

		def clusteringScore(data: RDD[Vector], k: Int) =
		{
			val kMeans = new KMeans()
			kMeans.setK(k)
			//kMeans.setRuns(10)
			//kMeans.setEpsilon(1.0e-6)

			val model = kMeans.run(data)
			data.map(datum => distToCentroid(datum, model)).mean()
		}

		(5 to 15 by 5).map(k => (k, clusteringScore(scaledData, k))).toList.foreach(println)
*/


	}
}