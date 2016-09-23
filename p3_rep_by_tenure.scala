import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml.XML


object p3 {
    def parse(col: List[String], str_to_parse: String)  = {
        val xml_to_parse =  XML.loadString(str_to_parse) 
        val vals = col.map( x => (xml_to_parse \ ("@" + x) ).text )
        (vals(0), vals(1))
        }
    def isvalidXml(str: String) : Boolean = { str.contains("<row") && str.contains("/>") }


    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("p3")
        val spark = new SparkContext(conf)
        val DIR = "file:///home/vagrant/miniprojects/spark/stats/"

        val post_columns = List("OwnerUserId", "PostTypeId") 
        val user_columns = List("Id", "Reputation")

        val posts_data = spark.textFile(DIR + "allPosts/*")
            .filter(isvalidXml)
            .map(parse(post_columns, _))
            .map(t => (t._1, 1)) // just getting the ID count, not using PostTypeId 
            .reduceByKey(_ + _)

        val users_data = spark.textFile(DIR + "allUsers/*")
            .filter(isvalidXml) 
            .map(parse(user_columns, _))

        val joined_data = posts_data.join(users_data)
            .map{case (k, v) => (v._1, v._2.toDouble)}
            .combineByKey( (v) => (v, 1), 
                           (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),
                           (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
                           )
            .map{ case (k, v) => (k, v._1/v._2) }
            .sortByKey(false)
        val ans = joined_data.take(100).toList
        println(ans)

    }
}
