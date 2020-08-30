package com.learningspark.example
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object WordCount {
  
  def main( args: Array[String] ) {
    
    // set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // create 'Spark Context' object
    val sc = new SparkContext( new SparkConf().setAppName("Spark Word Count").setMaster("local") )
    
    // read each line of the text file
    val input = sc.textFile("./news-2016-2017.txt")
    
    // split the sentences into word with 'empty space'
    val words = input.flatMap( x=> x.split(" ") )
    
    val wordCounts = words.countByValue()
    
    // print the results
//    wordCounts.foreach( println )
    val output1 = wordCounts.toSeq.sortBy(_._2)
    output1.foreach( println )
    
  } // def main
  
} // object