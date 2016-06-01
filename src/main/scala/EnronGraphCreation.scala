import java.util.UUID

import com.twitter.chill.Tuple3Serializer
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by frank on 01/06/16.
  */
object EnronGraphCreation extends App{


    // New SparkContext
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[2]")
      .setAppName("EnronGraphCreation")
    )

  val sentMails = sc.wholeTextFiles("\"hdfs://master.spark.com/Enron/maildir/allen-p/_sent_mail/")

  println("\n il y a "+sentMails.count()+" mail envoyés par allen-p \n")


}
