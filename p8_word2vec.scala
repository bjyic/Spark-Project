import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml.XML
import scala.util.matching.Regex
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}


object p8 {
    def parse(str_to_parse: String)  = {
        val xml_to_parse =  XML.loadString(str_to_parse) 
        val vals = getTag((xml_to_parse \ "@Tags").text)
        vals
    }

    def isvalidXml(str: String) : Boolean = { 
        str.contains("<row") && str.contains("/>") 
    }

    def getTag(text: String): String = {
        ("<(.*?)>"r).findAllMatchIn(text).map( x => x.group(1)) mkString " "
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("p8")
        val spark = new SparkContext(conf)
        val DIR = "file:///home/vagrant/miniprojects/spark/"

        val OUT_DIR = "file:///home/vagrant/miniprojects/spark/tags/"
//        val post_xmls = spark.textFile(DIR + "allPosts/part-*.xml.gz")
//                    .filter(isvalidXml)
//        val input = post_xmls.map(parse(_))
//        input.saveAsTextFile(OUT_DIR)

        val word2vec = new Word2Vec()
        word2vec.setSeed(42L)

        val texts = spark.textFile(DIR + "tags/part-*")
        val input = texts.map(x => x.split(" ").toSeq)
        val model = word2vec.fit(input)

        val synonyms = model.findSynonyms("ggplot2", 100)

        println(synonyms.toList)
    }
}
