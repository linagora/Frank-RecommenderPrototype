import java.util.UUID

import com.twitter.chill.Tuple3Serializer
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank
  */
object EnronCleanup extends App{

  // New SparkContext
  val sc = new SparkContext(new SparkConf()
    // local 8 means local machine 8 threads, optimal with 8 core CPU
    // yarn-local : on the cluster with local machine as master and default output terminal
    // yarn-cluster : on the cluster with best setup but no output on terminal, must look at logs of each machine
    .setMaster("local[8]")
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

  val usersReceivedMails  : VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
  val usersSentMails      : VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)



  // printing tests
  println("num edges = " + graph.numEdges)
  println("num vertices = " + graph.numVertices)
}