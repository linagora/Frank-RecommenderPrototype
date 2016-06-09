import java.util.UUID

import com.twitter.chill.Tuple3Serializer
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
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
  val users = new ListBuffer[String]
  val fromUsers = new ListBuffer[String]
  val anonymousGroup = new ListBuffer[Array[String]]


  // RFC Standard
  val mailPattern = "(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])".r
  val listEdges = new ListBuffer[(Int, Int, String)]

  sentMails.collect().foreach(f = mail => {
    val toLine = mail.split("\n").filter(line => line.contains("To: ")).head
    val ccLine = mail.split("\n").filter(line => line.contains("cc: ")).head
    val fromLine = mail.split("\n").filter(line => line.contains("From: ")).head
    val from: String = (mailPattern findFirstIn fromLine).get
    if (!users.contains(from)) {
      users.append(from)
    }
    if (!fromUsers.contains(from)) {
      fromUsers.append(from)
    }
    // First let's take care of the To section
    // We add users to the list if they are not already in
    val toArray: Array[String] = (mailPattern findAllIn toLine).toArray.map(toUser => {
      if (!users.contains(toUser)) {
        users.append(toUser)
      }
      toUser
    })
    // If there is just 1 to, make a direct link between from and to
    if (toArray.length<2) {
      for (to <- toArray) {
        listEdges.append((users.indexOf(from), users.indexOf(to), "to"))
      }
    }
      // else create an anonymous node and make a link from-node, node-tos
    else{
      if (!anonymousGroup.contains(toArray)){
        anonymousGroup.append(toArray)
      }
      listEdges.append((users.indexOf(from),anonymousGroup.indexOf(toArray)+10000,"to"))
      for (to <- toArray) {
        listEdges.append((anonymousGroup.indexOf(toArray)+10000, users.indexOf(to), "to"))
      }
    }
    // We take care of the CC section
    val ccArray: Array[String] = (mailPattern findAllIn ccLine).toArray.map(ccUser => {
      if (!users.contains(ccUser)) {
        users.append(ccUser)
      }
      ccUser
    })
    // If there is just 1 to, make a direct link between from and cc
    if (ccArray.length<2) {
      for (cc <- ccArray) {
        listEdges.append((users.indexOf(from), users.indexOf(cc), "cc"))
      }
    }
    // else create an anonymous node and make a link from-node, node-ccs
    else{
      if (!anonymousGroup.contains(ccArray)){
        anonymousGroup.append(ccArray)
      }
      listEdges.append((users.indexOf(from),anonymousGroup.indexOf(ccArray)+10000,"to"))
      for (cc <- ccArray) {
        listEdges.append((anonymousGroup.indexOf(ccArray)+10000, users.indexOf(cc), "to"))
      }
    }
  })

  val tripleRDD : RDD[(Int,Int,String)] = sc.parallelize(listEdges)

  // Replace arc string by count we use -1 to get shortest path to
  val triplesArcCountRDD:RDD[(Int,Int,Int)] = tripleRDD.map(triple =>(triple._1+""+triple._2,(triple._1,triple._2,-1))).reduceByKey((triple1,triple2)=>(triple1._1,triple1._2,triple1._3+triple2._3)).map(_._2)

  //Create Triples Edges
  val edgesRDD = triplesArcCountRDD.map(triple => Edge(triple._1,triple._2.hashCode,triple._3))

  // Create the Graph
  val graph = Graph.fromEdges(edgesRDD, "defaultProperty")

  val usersReceivedMails  : VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
  val usersSentMails      : VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)


  // ShortestPaths to
  val currentUser = 10
  val dest = 11
  val result = ShortestPaths.run(graph, Seq(currentUser))
  val shortestPath = result               // result is a graph
    .vertices                             // we get the vertices RDD
    .filter({case(vId, _) => vId == currentUser})  // we filter to get only the shortest path from v1
    .first                                // there's only one value
    ._2                                   // the result is a tuple (v1, Map)
    .get(dest)

  //printings
  println("\n Shortest path between "+currentUser+ " and "+dest+" is "+shortestPath.toString)
  sc.stop()
}
