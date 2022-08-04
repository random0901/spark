import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object wordcount{
	def main(args:Array[String]){
		val conf=new SparkConf().setAppName("wordcount").setMaster("local")
		val sc=new SparkContext(conf)
		val pathtofile="log.txt"
		val wordsRDD=sc.textFile(pathtofile).flatMap(_.split(" "))
		val wordsinitRDD=wordsRDD.map(x => (x,1))
		val wc=wordsinitRDD.reduceByKey((v1,v2) => v1+v2).filter(x => x._2 > 4)
		wc.saveAsTextFile("wodcountsdir")
	}
}
