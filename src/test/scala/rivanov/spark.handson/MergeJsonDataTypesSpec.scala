package rivanov.spark.handson

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import spray.json.{JsNumber, JsArray, JsString, JsObject}

/**
 * Created by roman on 5/19/15.
 */
@RunWith(classOf[JUnitRunner])
class MergeJsonDataTypesSpec extends Specification {

  sequential

  "Json documents from two different sources " should {

    val sparkConf = new SparkConf()
    val sc = new SparkContext("local[*]", "JsonMerge", sparkConf)

    "when have different data types" in {
      val j1 = TypeAProvider.produceJson(1).head
      val j2 = TypeBProvider.produceJson(1).head
      j1.fields.keySet.forall(j2.fields.keySet.contains) must beFalse
    }

    "could be transformed to unified format" in {
      val rddA: RDD[JsObject] = sc.parallelize(TypeAProvider.produceJson(10)).map {
        json => {
          println("RDD A: " + json.toString())
          JsObject(
            ("id", JsNumber(System.nanoTime())),
            ("source", json.fields("type")),
            ("data", json.fields("data"))
          )
        }
      }

      val rddB: RDD[JsObject] = sc.parallelize(TypeBProvider.produceJson(10)).map {
        json => {
          println("RDD B: " + json.toString())
          JsObject(
            ("id", JsNumber(System.nanoTime())),
            ("source", json.fields("type")),
            ("data", json.fields("payload").asJsObject.fields("data"))
          )
        }
      }

      val merged = sc.union(rddA, rddB)

      val result: Array[(String, Iterable[JsObject])] = merged.groupBy(json => json.fields("source").asInstanceOf[JsString].value).collect()

      result.length must_== 2
      result.forall(_._2.size == 10) must beTrue

      val checkGroupsFormat: (String, Iterable[JsObject]) => Boolean = (dataType, group) => group.forall {
        json => {
          println(s"RDD unified $dataType: " + json.toString())
          json.fields.size == 3 &&
            json.fields("id").asInstanceOf[JsNumber].value < System.nanoTime() &&
            json.fields("source").asInstanceOf[JsString].value == dataType &&
            json.fields("data").asInstanceOf[JsArray].elements.forall(_.asInstanceOf[JsString].value.startsWith(s"$dataType."))
        }
      }

      result.forall(checkGroupsFormat.tupled)
    }

    "then context could be stopped" in {
      sc.stop()
      success
    }

  }

}
