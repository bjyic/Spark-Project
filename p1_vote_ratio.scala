import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml.XML
// why does case class work and regular class doesn't?
case class Vote(val pid: String, val vtype: Int) {
    val postid = pid
    var (upvotes, downvotes, favorites):(Int, Int, Int) = vtype match {
        case 2 => (1, 0, 0)
        case 3 => (0, 1, 0)
        case 5 => (0, 0, 1)
        case _ => (0, 0, 0)
    }

    def +(that: Vote): Vote = {
        upvotes += that.upvotes
        downvotes += that.downvotes
        favorites += that.favorites
        this
    }

    override def toString(): String = {
        "(" +postid+":"+ upvotes + " " + downvotes + " " + favorites + ")"
    }
}

object p1 {
    def parse(str_to_parse: String)  = {
        val xml_to_parse = XML.loadString(str_to_parse)
        val postid = (xml_to_parse \ "@PostId").text
        val votetypeid = (xml_to_parse \ "@VoteTypeId").text.toInt
        val v = new Vote(postid, votetypeid)
        (postid, v)
    }

    def getRatio(v : Vote): (Int, Double) = {
        val n = v.upvotes + v.downvotes
        (v.favorites, if (n == 0) 0.0 else 1.0*v.upvotes/n)
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("p1_vote_ratio")
        val spark = new SparkContext(conf)
        val res = spark.textFile("file:///home/vagrant/miniprojects/spark/allVotes/part-0*")
            .filter(x => x.contains("VoteTypeId"))
            .map(parse)
            .reduceByKey(_ + _)
        val ans = res.map(_._2)
            .map(getRatio)
            .combineByKey(
              (v) => (v, 1),
              (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),
              (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
              ).map{case (key, value) => (key, value._1 / value._2)}
            .sortBy(_._1)
            .take(50)
            .toList
        println(ans)

    }

}
