import java.util.UUID

import com.twitter.chill.Tuple3Serializer
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by frank on 01/06/16.
  */
object EnronGraphCreation extends App{


    // New SparkContext
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[2]")
      .setAppName("EnronGraphCreation")
    )

  val sentMails = sc.wholeTextFiles("hdfs://master.spark.com/Enron/maildir/*/_sent_mail/*").map(_._2)
val nbUsers = new ListBuffer[String]
  val tripleRDD = sentMails.collect().flatMap(mail=>{
    val toLine = mail.split("\n").filter(line=> line.contains("To: "))
    val ccLine = mail.split("\n").filter(line=> line.contains("cc: "))
    val fromLine = mail.split("\n").filter(line=> line.contains("From: "))
    val from = fromLine.head.split(" ")(1).hashCode
    val toArray = toLine.head.split("To: ")(1).split(",")
    val ccArray = ccLine.head.split("cc: ")(1).split(",")

    val listEdges = new ListBuffer[(Int,String,String)]
    for (to <- toArray){
      listEdges.append((from,to,"to"))
    }
    for (cc <- ccArray){
      listEdges.append((from,cc,"cc"))
    }
  if (!nbUsers.contains(fromLine.head.split(" ")(1))){
    nbUsers.append(fromLine.head.split(" ")(1))
  }

  listEdges.toList
  })

  val edgesRDD = sc.parallelize(tripleRDD).map(triple => Edge(triple._1,triple._2.hashCode,triple._3))

  // Create the Graph
  val graph = Graph.fromEdges(edgesRDD, "defaultProperty")

  val usersReceivedMails  : VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
  val usersSentMails      : VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)



  // printing tests
  println("\n il y a "+sentMails.count()+" mail envoyés \n")
  println("\nnum edges = " + graph.numEdges +"\n")
  println("\nnum vertices = " + graph.numVertices+"\n")
  println("\nthere are "+ nbUsers.size+ " users in this dataset\n")


}
