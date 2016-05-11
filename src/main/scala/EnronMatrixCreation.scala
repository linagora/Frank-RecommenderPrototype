import java.util.ArrayList

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank on 10/05/16.
  */
object EnronMatrixCreation extends App{

  val sc = new SparkContext(new SparkConf()
    .setMaster("local[2]")
    .setAppName("EnronMatrixCreation")
  )

  type EnronRow = (Int,Int,Int)


  val EnronRDD : RDD[EnronRow] = sc.textFile("hdfs://master.spark.com/Enron/TimeFromToDataset/execs.email.linesnum")
    .map(line => {
      val lineArray =  line.split(" ")
      (lineArray(0).toInt,lineArray(1).toInt,lineArray(2).toInt)
    })

  val EnronReceivedMailRDD: RDD[(Int,Iterable[EnronRow])] = EnronRDD.groupBy(_._2)
  val EnronSentMailRDD : RDD[(Int,Iterable[EnronRow])]= EnronRDD.groupBy(_._3)

  //create one row between each mail dent by the user
  val matrix : ArrayList[Array[Int]] = new ArrayList[Array[Int]]
  var index = 0
  val userReceivedMail:Array[EnronRow] =  EnronReceivedMailRDD.collect().toMap.get(25).get.toArray
  val row: Array[Int] = Array.fill[Int](185)(0)
  EnronSentMailRDD.collect().toMap.get(25).get.foreach(sentMail => {
    val sentMailTime = sentMail._1
    while ( sentMailTime > userReceivedMail(index)._1 ) {
      row(userReceivedMail(index)._2)+=1
      index+=1
    }
    row(sentMail._3)-=1
    row(185)=sentMail._3
    matrix.add(row)
  })

  println("\n Taille de la matrice " + matrix.size() + "\n")



  // for each mail sent, the row represents all current inbox mails that are waiting for and answer
  // we have a surpervised problem: input is mails in box waiting for answer and output is to whom the answer has to be sent first
}
