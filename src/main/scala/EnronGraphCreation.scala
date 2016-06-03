import java.util.UUID

import com.twitter.chill.Tuple3Serializer
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex


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
  val fromUsers = new ListBuffer[String]
  val toUsers = new ListBuffer[String]
  val ccUsers = new ListBuffer[String]

  // RFC Standard
  val mailPattern = "(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])".r

  val tripleRDD = sentMails.collect().flatMap(mail=>{
    val toLine = mail.split("\n").filter(line=> line.contains("To: ")).head
    val ccLine = mail.split("\n").filter(line=> line.contains("cc: ")).head
    val fromLine = mail.split("\n").filter(line=> line.contains("From: ")).head
    val from : String = (mailPattern findFirstIn fromLine).get
    val toArray : Array[String] = (mailPattern findAllIn toLine).toArray.map(toUser=>{
      toUsers.append(toUser)
      toUser
    })
    val ccArray : Array[String] = (mailPattern findAllIn ccLine).toArray.map(ccUser=>{
      ccUsers.append(ccUser)
      ccUser
    })
    val listEdges = new ListBuffer[(Int,Int,String)]
    if (!fromUsers.contains(from)) {
      fromUsers.append(from)
      for (to <- toArray) {
        listEdges.append((fromUsers.indexOf(from), toUsers.indexOf(to), "to"))
      }
      for (cc <- ccArray) {
        listEdges.append((fromUsers.indexOf(from), ccUsers.indexOf(cc), "cc"))
      }
    }

    listEdges.toList
  })

  // Replace arc string by count
  val triplesArcCountRDD:RDD[(Int,Int,Int)] = sc.parallelize(tripleRDD).map(triple =>(triple._1+""+triple._2,(triple._1,triple._2,1))).reduceByKey((triple1,triple2)=>(triple1._1,triple1._2,triple1._3+triple2._3)).map(_._2)

  //Create Triples Edges
  val edgesRDD = triplesArcCountRDD.map(triple => Edge(triple._1,triple._2.hashCode,triple._3))

  // Create the Graph
  val graph = Graph.fromEdges(edgesRDD, "defaultProperty")

  val usersReceivedMails  : VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
  val usersSentMails      : VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)



  // printing tests
  println("\n il y a "+sentMails.count()+" mail envoyés \n")
  println("\nnum edges = " + graph.numEdges +"\n")
  println("\nnum vertices = " + graph.numVertices+"\n")
  //println("\nthere are "+ fromUsers.size + " users in this dataset\n")
  println("\nthere are : "+usersSentMails.count()+" users that sent emails\n")
  println("\nthere are : "+usersReceivedMails.count()+" users that received emails\n")

  //println(fromUsers.mkString("\n"))
  println("\nfromUsers count :"+fromUsers.length)
  println("toUsers count :"+toUsers.length)
  println("ccUsers count :"+ccUsers.length)


}
