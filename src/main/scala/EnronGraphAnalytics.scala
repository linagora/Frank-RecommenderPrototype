import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank on 13/06/16.
  *
  * Load graph form HDFS, loses of email string information only ids
  */
object EnronGraphAnalytics extends App{

  // New SparkContext
  val sc = new SparkContext(new SparkConf()
    // local 8 means local machine 8 threads, optimal with 8 core CPU
    // yarn-local : on the cluster with local machine as master and default output terminal
    // yarn-cluster : on the cluster with best setup but no output on terminal, must look at logs of each machine
    .setMaster("local[8]")
    .setAppName("EnronGraphAnalytics")
  )
  val graph = GraphLoader.edgeListFile(sc,"hdfs://master.spark.com/Enron/GraphEdges")

  val destid= 55
  val senderId = 25
  val id=  graph.edges
    // Select the user dest user and the source user
    .filter(row => (row.dstId == destid && (row.srcId == senderId || row.srcId>9999)))
    // Sort recipient users by number of exchange
    .sortBy(_.attr)
    .first().srcId

  if (id>9999) {
    val recommendedUserArray = graph.edges
      .filter(_.srcId == id).map(_.dstId).collect()
    println("\nRecommend to send mails to : "+recommendedUserArray.mkString(" ; ")+"\n")
  }
  else{
    println("\nSend direct Mail to "+destid+"\n")
  }
}
