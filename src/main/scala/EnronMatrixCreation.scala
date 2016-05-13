import java.util.ArrayList

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank
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

  val userReceivedMail=  EnronReceivedMailRDD.collect().filter(_._1==25).head._2.toArray

  val row: Array[Int] = Array.fill[Int](185)(0)

  val toto = EnronSentMailRDD.collect().filter(_._1==25).head._2/*.foreach( sentMail => {

    val sentMailTime = sentMail._1
    if (index < userReceivedMail.size ){
      while (sentMailTime > userReceivedMail(index)._1) {
        row(userReceivedMail(index)._2) += 1
        index += 1
      }
    }
    row(sentMail._3)-=1
    row(185)=sentMail._3
    matrix.add(row)

  })
  */

  println("\n Taille de la matrice " + matrix.size() + "\n")
  println("\n Taille de userRceivedMail 25 : " + userReceivedMail.size + "\n")
  println("\n Taille de toto : " + toto.size + "\n")


}
