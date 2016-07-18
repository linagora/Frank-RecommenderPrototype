
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by frank
  */
object EnronMatrixCreation extends App{

  // New SparkContext
  val sc = new SparkContext(new SparkConf()
    // local 8 means local machine 8 threads, optimal with 8 core CPU
    // yarn-local : on the cluster with local machine as master and default output terminal
    // yarn-cluster : on the cluster with best setup but no output on terminal, must look at logs of each machine
    .setMaster("local[8]")
    .setAppName("EnronMatrixCreation")
  )

  type EnronRow = (Int,Int,Int)


  val EnronRDD : RDD[EnronRow] = sc.textFile("hdfs://master.spark.com/Enron/TimeFromToDataset/execs.email.linesnum")
    .map(line => {
      val lineArray =  line.split(" ")
      (lineArray(0).toInt,lineArray(1).toInt,lineArray(2).toInt)
    })

  //create one row between each mail dent by the user
  val matrix: ListBuffer[Array[Int]] = new ListBuffer[Array[Int]]()

  // Mails sent TO user
  val EnronReceivedMailRDD: RDD[(Int,Iterable[EnronRow])] = EnronRDD.groupBy(_._3)
  //mails SENT BY (FROM) user
  val EnronSentMailRDD: RDD[(Int, Iterable[EnronRow])] = EnronRDD.groupBy(_._2)


    //mails sent To user i
    val userReceivedMail = EnronReceivedMailRDD.collect().filter(_._1 == 25).head._2.toArray.sortBy(_._1)
    // mails SENT By (FROM) user i
    val userSentMails: Array[EnronRow] = EnronSentMailRDD.collect().filter(_._1 == 25).head._2.toArray.sortBy(_._1)

    var index = 0

    val row: Array[Int] = Array.fill[Int](184)(0)

    for (receivedMail <- userReceivedMail) {
      //userSentMails.foreach( sentMail => {

      val receivedmailTime = new Date(receivedMail._1)
      if (index <= userReceivedMail.size) {
        if (receivedmailTime.before(new Date(userSentMails(index)._1))) {
          row(userReceivedMail(index)._2) += 1
        }
        else {
          row(userSentMails(index)._3) -= 1
          row(183) = userSentMails(index)._3
          matrix.append(row)
          index+=1
        }
      }
    }

  sc.parallelize(matrix).saveAsTextFile("hdfs://master.spark.com/Enron/MatrixResult")
  sc.parallelize(matrix.map(_.mkString(" , "))).saveAsTextFile("hdfs://master.spark.com/Enron/MatrixResultString")
}

//Append le dernier row a chaque fois au lieu d'append chaque row 1 par 1