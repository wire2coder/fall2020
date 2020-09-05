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
    val textfile1 = sc.textFile("./news-2016-2017.txt")
//    val textfile1 = sc.textFile("./readme1.txt")
    
    // split the sentences into word with 'empty space'
    val words = textfile1.flatMap( asdf => asdf.split(" ") ) 
      .map( asdf => (asdf.toLowerCase) )
      .map( asdf => (asdf, 1) )
      .reduceByKey( (x,y) => x+y) // combined value with the SAME KEY
 
      // rdd.sortByKey(ascending=True, numPartitions=None, keyfunc = lambda x: str(x))
      // https://www.oreilly.com/library/view/learning-spark/9781449359034/ch04.html
    val output2 = words.map( asdf => (asdf._2, asdf._1) ).sortByKey(false)  
      
    val output3 = output2.take(500)
    output3.foreach(println)

        
  } // def main
  
} // object