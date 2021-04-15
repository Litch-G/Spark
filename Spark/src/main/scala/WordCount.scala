import java.io.{File, FileOutputStream, PrintWriter}
import java.util
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc =new SparkContext(config)
    //print(sc)
    import java.util
    val files = new util.ArrayList[String]
    val file = new File("F:\\upload")
    val tempList:Array[File] = file.listFiles()
    var i = 0
    if (tempList.length>0){
    for ( i<- 0 until  tempList.length){
      if(tempList(i).isFile){
        val lines: RDD[String] = sc.textFile(tempList(i).toString)
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne: RDD[(String, Int)] = words.map((_,1))
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        val wordToSorted: RDD[(String, Int)] = wordToSum.sortBy(_._2,false)
        val result: Array[(String, Int)] = wordToSorted.collect()
        result.foreach(println)
        val temp:Array[String] = tempList(i).toString().split("\\\\")
        val os = new FileOutputStream("f:\\userInfo\\"+temp(2))
        val pw = new PrintWriter(os)
        pw.println()
        result.foreach(pw.println)
        pw.close
        tempList(i).delete()
      }
    }
    }
    sc.stop()
  }
}
