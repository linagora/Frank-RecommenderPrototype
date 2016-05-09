import java.util.UUID

import com.twitter.chill.Tuple3Serializer
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank
  */
object EnronCleanup{

  // New SparkContext
  val sc = new SparkContext(new SparkConf()
    .setMaster("local[2]")
    .setAppName("EnronCleanup")
  )

  // Reading Enron Dataset
  val EnronEdgesRDD : RDD[Edge[String]] = sc.textFile("hdfs://master.spark.com/Enron/TimeFromToDataset/execs.email.linesnum")
                                      .map(line => {
                                       val lineArray =  line.split(" ")
                                        Edge(lineArray(1).toLong,lineArray(2).toLong,lineArray(0))
                                      })

  // Create the Graph
  val graph = Graph.fromEdges(EnronEdgesRDD, "defaultProperty")

  // printing tests
  println("num edges = " + graph.numEdges)
  println("num vertices = " + graph.numVertices)

}
