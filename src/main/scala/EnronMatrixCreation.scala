import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank on 10/05/16.
  */
object EnronMatrixCreation extends App{

  val sc = new SparkContext(new SparkConf()
    .setMaster("local[2]")
    .setAppName("EnronCleanup")
  )

  type EnronRow = (Int,Int,Int)

  def enronRow(time:Int,from:Int,to:Int) : EnronRow = {
    (time,from,to)
  }

  val EnronRDD : RDD[EnronRow] = sc.textFile("hdfs://master.spark.com/Enron/TimeFromToDataset/execs.email.linesnum")
    .map(line => {
      val lineArray =  line.split(" ")
      enronRow(lineArray(0).toInt,lineArray(1).toInt,lineArray(2).toInt)
    })

  val EnronReceivedMailRDD: RDD[(Int,Iterable[EnronRow])] = EnronRDD.groupBy(_._2)
  val EnronSentMailRDD : RDD[(Int,Iterable[EnronRow])]= EnronRDD.groupBy(_._3)

  //create one row between each mail dent by the user
  val matrix = new util.ArrayList[Array[Int]]
  var index = 0
  EnronSentMailRDD.collect().toMap.get(25).get.foreach(sentMail => {
    val row: Array[Int] = Array.fill[Int](184)(0)
    val sentMailTime = sentMail._1
    val userReceivedMail = EnronReceivedMailRDD.collect().toMap.get(25).get.toArray
    while ( sentMailTime > userReceivedMail(index)._1 ) {
      row(userReceivedMail(index)._2)+=1
      index+=1
    }
    matrix.add(row)
  })

  println("\n Taille de la matrice " + matrix.size()+"\n")



  // for each mail sent, the row represents all current inbox mails that are waiting for and answer
  // we have a surpervised problem: input is mails in box waiting for answer and output is to whom the answer has to be sent first
}
