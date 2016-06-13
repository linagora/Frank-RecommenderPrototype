import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank on 13/06/16.
  */
object EnronGraphAnalytics extends App{

  // New SparkContext
  val sc = new SparkContext(new SparkConf()
    .setMaster("local[2]")
    .setAppName("EnronCleanup")
  )


  val graph = GraphLoader.edgeListFile(sc,"hdfs://master.spark.com/Enron/GraphEdges")


  // printing tests
  println("num edges = " + graph.numEdges)
  println("num vertices = " + graph.numVertices)

}
