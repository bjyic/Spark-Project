import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml.XML
import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime
import org.joda.time.LocalDateTime
import org.joda.time.DateTimeZone
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

// same code is used for running p5

object p4 {
    def parse(col: List[String], str_to_parse: String)  = {
        val xml_to_parse =  XML.loadString(str_to_parse) 
        val vals = col.map( x => (xml_to_parse \ ("@" + x) ).text )
        (vals(0), vals(1))
        }
    def isvalidXml(str: String) : Boolean = { str.contains("<row") && str.contains("/>") }
    
    def getHourDiff(dates: (String, String)): (Int, Int) = {
        val pattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
                                    .withZone(DateTimeZone.UTC)
        val t1 = pattern.parseDateTime(dates._1)
        val t2 = pattern.parseDateTime(dates._2)
        val diff = new Duration(t1, t2)
        val h = diff.getStandardHours()
        (t1.getHourOfDay(), if (h>= 3) 0 else 1)
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("p4")
        val spark = new SparkContext(conf)
        val DIR = "file:///home/vagrant/miniprojects/spark/"

        val question_columns = List("AcceptedAnswerId", "CreationDate")
        val ans_columns = List("Id", "CreationDate")

        val xmls = spark.textFile(DIR + "allPosts/part-*.xml.gz")
             .filter(isvalidXml)

        val questions = xmls.filter(x => x.contains("AcceptedAnswerId"))
             .map(parse(question_columns, _))
        val answers = xmls.filter(x => !x.contains("AcceptedAnswerId"))
             .map(parse(ans_columns, _))
        val ans = questions.join(answers)
                           .map{case (k, v) => getHourDiff(v)}
                           .combineByKey( (v) => (v, 1),
                               (acc:(Int, Int), v) => (acc._1 + v, acc._2 + 1),
                               (acc1:(Int, Int), acc2:(Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
                           ).map{ case (k, v) => (k, 1.0*v._1/v._2) }
                           .sortByKey()
                           .collect()
                           .toList
                           .map(_._2)

        println(ans)
    }
}
