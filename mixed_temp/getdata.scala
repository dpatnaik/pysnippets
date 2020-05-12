import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.graphx._

import scala.io.Source
import scala.util.control.Breaks._

object getDataObj
{
	def main(args: Array[String]): Unit =
	{
		val sc = new SparkContext(new SparkConf().setAppName("GraphData"))

		val graph = GraphLoader.edgeListFile(sc, "file:///C:/Users/dpatn/Desktop/Win_Share/projects/Winklrtest/data/orig_train.csv")
		val baseRDD = sc.textFile("file:///C:/Users/dpatn/Desktop/Win_Share/projects/Winklrtest/data/orig_train.csv")
		val testRDD = sc.textFile("file:///C:/Users/dpatn/Desktop/Win_Share/projects/Winklrtest/data/orig_test.csv")

		graph.cache()

		val mapFile = "C:/Users/dpatn/Desktop/Win_Share/projects/Winklrtest/data/testnodesmap.txt"
		val tNodesMap = scala.collection.mutable.Map[Long, Array[Long]]()
		for (x <- Source.fromFile(mapFile).getLines())
		{
			val entries = x.split(':')
			tNodesMap += (entries(0).toLong -> entries(1).split(',').map(_.toLong))
		}

		val testNodesMap = sc.broadcast(tNodesMap)
		val pageRank = sc.broadcast(graph.pageRank(0.0001).vertices.collect().toMap)
		val followees = sc.broadcast(graph.collectNeighborIds(EdgeDirection.Out).collect().toMap)
		val followers = sc.broadcast(graph.collectNeighborIds(EdgeDirection.In).collect().toMap)
		val neighbors = sc.broadcast(graph.collectNeighborIds(EdgeDirection.Either).collect().toMap)
		val degrees = sc.broadcast(graph.degrees.collect().toMap)

		val posRecs = baseRDD.map
		{
			record =>
			val nodes = record.split(' ')
			
			val srcNode = nodes(0).toLong
			val desNode = nodes(1).toLong

			val srcPR = "%.5f".format(pageRank.value(srcNode))
			val desPR = "%.5f".format(pageRank.value(desNode))

			val srcFollowees = followees.value(srcNode).toSet
			val srcFollowers = followers.value(srcNode).toSet
			val srcNeighbors = neighbors.value(srcNode).toSet
			val desFollowees = followees.value(desNode).toSet
			val desFollowers = followers.value(desNode).toSet
			val desNeighbors = neighbors.value(desNode).toSet

			var desFollowsSrc = 0
			if (desFollowees.contains(srcNode))
				desFollowsSrc = 1

			val js_src_followees_des_followers = "%.5f".format(srcFollowees.intersect(desFollowers).size.toFloat / srcFollowees.union(desFollowers).size.toFloat)
			val js_src_followers_des_followers = "%.5f".format(srcFollowers.intersect(desFollowers).size.toFloat / srcFollowers.union(desFollowers).size.toFloat)
			val js_src_followees_des_followees = "%.5f".format(srcFollowees.intersect(desFollowees).size.toFloat / srcFollowees.union(desFollowees).size.toFloat)
			val js_src_followers_des_followees = "%.5f".format(srcFollowers.intersect(desFollowees).size.toFloat / srcFollowers.union(desFollowees).size.toFloat)

			var adamic = 0.0
			val cmnNeighbors = srcNeighbors.intersect(desNeighbors)
			for (x <- cmnNeighbors)
			{
				if(x != srcNode && x != desNode)
				{
					if(degrees.value(x) != 0)
						adamic += 1.0 / scala.math.log(x)
				}
			}

			"1," + srcNode + "," + desNode + "," + srcPR + "," + desPR + "," + desFollowsSrc + "," + js_src_followees_des_followers + "," + js_src_followers_des_followers + "," + js_src_followees_des_followees + "," + js_src_followers_des_followees + "," + adamic
		}

		val negRecs = baseRDD.map
		{
			record =>
			val nodes = record.split(' ')
			
			val srcNode = nodes(0).toLong
			var desNode = nodes(1).toLong

			var newDesNode = 0L
			val noLinksNodes = neighbors.value(desNode).toSet.diff(followees.value(srcNode).toSet)

			breakable
			{
				for(x <- noLinksNodes)
				{
					if(!testNodesMap.value.get(srcNode).isEmpty)
					{
						if(!testNodesMap.value(srcNode).contains(x) && srcNode != x && desNode != x)
						{
							newDesNode = x
							break
						}
					}
					else
					{
						newDesNode = x
						break
					}
				}
			}

			desNode = newDesNode
			if(desNode != 0)
			{
				val srcPR = "%.5f".format(pageRank.value(srcNode))
				val desPR = "%.5f".format(pageRank.value(desNode))

				val srcFollowees = followees.value(srcNode).toSet
				val srcFollowers = followers.value(srcNode).toSet
				val srcNeighbors = neighbors.value(srcNode).toSet
				val desFollowees = followees.value(desNode).toSet
				val desFollowers = followers.value(desNode).toSet
				val desNeighbors = neighbors.value(desNode).toSet

				var desFollowsSrc = 0
				if (desFollowees.contains(srcNode))
					desFollowsSrc = 1

				val js_src_followees_des_followers = "%.5f".format(srcFollowees.intersect(desFollowers).size.toFloat / srcFollowees.union(desFollowers).size.toFloat)
				val js_src_followers_des_followers = "%.5f".format(srcFollowers.intersect(desFollowers).size.toFloat / srcFollowers.union(desFollowers).size.toFloat)
				val js_src_followees_des_followees = "%.5f".format(srcFollowees.intersect(desFollowees).size.toFloat / srcFollowees.union(desFollowees).size.toFloat)
				val js_src_followers_des_followees = "%.5f".format(srcFollowers.intersect(desFollowees).size.toFloat / srcFollowers.union(desFollowees).size.toFloat)

				var adamic = 0.0
				val cmnNeighbors = srcNeighbors.intersect(desNeighbors)
				for (x <- cmnNeighbors)
				{
					if(x != srcNode && x != desNode)
					{
						if(degrees.value(x) != 0)
							adamic += 1.0 / scala.math.log(x)
					}
				}

				"0," + srcNode + "," + desNode + "," + srcPR + "," + desPR + "," + desFollowsSrc + "," + js_src_followees_des_followers + "," + js_src_followers_des_followers + "," + js_src_followees_des_followees + "," + js_src_followers_des_followees + "," + adamic
			}
			else
				"NA"
		}

		posRecs.union(negRecs).filter(l => l != "NA").saveAsTextFile("file:///C:/Users/dpatn/Desktop/Win_Share/projects/Winklrtest/data/reformed")

		val testRecs = testRDD.map
		{
			record =>
			val nodes = record.split(' ')
			
			val srcNode = nodes(0).toLong
			val desNode = nodes(1).toLong

			try
			{ 
			
				val srcPR = "%.5f".format(pageRank.value(srcNode))
				val desPR = "%.5f".format(pageRank.value(desNode))

				val srcFollowees = followees.value(srcNode).toSet
				val srcFollowers = followers.value(srcNode).toSet
				val srcNeighbors = neighbors.value(srcNode).toSet
				val desFollowees = followees.value(desNode).toSet
				val desFollowers = followers.value(desNode).toSet
				val desNeighbors = neighbors.value(desNode).toSet

				var desFollowsSrc = 0
				if (desFollowees.contains(srcNode))
					desFollowsSrc = 1

				val js_src_followees_des_followers = "%.5f".format(srcFollowees.intersect(desFollowers).size.toFloat / srcFollowees.union(desFollowers).size.toFloat)
				val js_src_followers_des_followers = "%.5f".format(srcFollowers.intersect(desFollowers).size.toFloat / srcFollowers.union(desFollowers).size.toFloat)
				val js_src_followees_des_followees = "%.5f".format(srcFollowees.intersect(desFollowees).size.toFloat / srcFollowees.union(desFollowees).size.toFloat)
				val js_src_followers_des_followees = "%.5f".format(srcFollowers.intersect(desFollowees).size.toFloat / srcFollowers.union(desFollowees).size.toFloat)

				var adamic = 0.0
				val cmnNeighbors = srcNeighbors.intersect(desNeighbors)
				for (x <- cmnNeighbors)
				{
					if(x != srcNode && x != desNode)
					{
						if(degrees.value(x) != 0)
							adamic += 1.0 / scala.math.log(x)
					}
				}

				srcNode + "," + desNode + "," + srcPR + "," + desPR + "," + desFollowsSrc + "," + js_src_followees_des_followers + "," + js_src_followers_des_followers + "," + js_src_followees_des_followees + "," + js_src_followers_des_followees + "," + adamic					
			}
			catch
			{
			  case e: Exception => record + " Error"
			}
		}

		testRecs.saveAsTextFile("file:///C:/Users/dpatn/Desktop/Win_Share/projects/Winklrtest/data/testData")		
	}
}