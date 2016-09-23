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


object p6 {
    def parse(col: List[String], str_to_parse: String)  = {
        val xml_to_parse =  XML.loadString(str_to_parse) 
        val vals = col.map( x => (xml_to_parse \ ("@" + x) ).text )
        (vals(0), vals.tail)
        }


    def isvalidXml(str: String) : Boolean = { str.contains("<row") && str.contains("/>") }
    
    def getHourDiff(date1: String, date2: String): Long = {
        val pattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
                                    .withZone(DateTimeZone.UTC)
        val t1 = pattern.parseDateTime(date1)
        val t2 = pattern.parseDateTime(date2)
        val diff = new Duration(t1, t2)
        diff.getStandardDays()
    }

    def getMinDate(date1: List[String], date2: List[String]): List[String] = {
        val pattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
                                    .withZone(DateTimeZone.UTC)
        val t1 = pattern.parseDateTime(date1(0))
        val t2 = pattern.parseDateTime(date2(0))
        if (t1.getMillis() < t2.getMillis())
            date1
        else
            date2
    }

    def isVeteran(date_tup: (List[String], Iterable[List[String]])): Boolean = {
        val pattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
                                    .withZone(DateTimeZone.UTC)
        val t1 = date_tup._1(0)
        val t2 = date_tup._2.map(_(0))
        val t2_days: Iterable[Long] = t2.map(getHourDiff(t1, _))
        t2_days.exists(d => (d >= 100 && d <= 150))
    
    }
    def option_toInt(in: String): Option[Int] = {
        try{ Some(in.toInt) } catch {case e : NumberFormatException => None } 
    }
    def add_option_Int(a: Option[Int], b: Option[Int]): Option[Int] = {
        (a ++ b).reduceOption(_ + _)
    }
    def some_to_1_none_to_0(v: Option[Int]): Int = { if (v == None) 0 else 1}


    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("p6")
        val spark = new SparkContext(conf)
        val DIR = "file:///home/vagrant/miniprojects/spark/"

        val post_columns = List("OwnerUserId", "CreationDate")
        val post_features_columns = List("OwnerUserId", "CreationDate", "ViewCount", "Score", "FavoriteCount", "AnswerCount", "PostTypeId")
        val user_columns = List("Id", "CreationDate")

        val post_xmls = spark.textFile(DIR + "allPosts/part-*.xml.gz")
             .filter(isvalidXml)
        val user_xmls = spark.textFile(DIR + "allUsers/part-*.xml.gz")
             .filter(isvalidXml)

        val posts = post_xmls.map(parse(post_columns, _))
        val users = user_xmls.map(parse(user_columns, _))
        val posts_features = post_xmls.map(parse(post_features_columns, _))
        
        val first_posts = posts_features.filter(x => x._2.last == "1")
          .map{ case (k, v) => (k, v.init) }
          .reduceByKey(getMinDate)

        val vet_users = users.join(posts.groupByKey())
                       .map{case (k, v) => (k, isVeteran(v))}

        val res = vet_users.join(first_posts)
          .map{ case (k, v) => (v._1, v._2.tail.map(option_toInt(_))) } 
          .combineByKey(
            (v) => (v, 1),
            (acc: (List[Option[Int]], Int), v: List[Option[Int]]) =>
                ((acc._1, v).zipped.map(add_option_Int(_,_)), acc._2+1), 
            (acc1: (List[Option[Int]], Int), acc2: (List[Option[Int]], Int)) =>
                ((acc1._1, acc2._1).zipped.map(add_option_Int(_,_)), acc1._2+acc2._2)
          ).map { case (k, v) => (k, v._1.map(_.getOrElse(0)*1.0/v._2)) }
          .collect().toList

        println(res)
    }
}
