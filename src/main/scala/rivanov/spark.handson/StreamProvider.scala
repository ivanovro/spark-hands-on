package rivanov.spark.handson

import spray.json.{JsObject, JsonParser}

/**
 * Created by roman on 5/19/15.
 */
trait JsonDataProvider {

  val produceJson: Int => Seq[JsObject] = n => for (i <- 0 until n) yield jsonDoc(i)

  def jsonDoc(id: Int): JsObject

}

object TypeAProvider extends JsonDataProvider {

  override def jsonDoc(i: Int) = JsonParser( s"""
                                                    {
                                                       "id": $i,
                                                       "type": "A",
                                                       "data": [
                                                         "A.${System.nanoTime()}",
                                                         "A.${System.nanoTime()}"
                                                       ]
                                                    }
                                                 """.stripMargin).asJsObject

}

object TypeBProvider extends JsonDataProvider {


  override def jsonDoc(i: Int) = JsonParser( s"""
                                                    {
                                                       "id": $i,
                                                       "type": "B",
                                                       "payload": {
                                                           "headers": {
                                                             "k1":"v1",
                                                             "k2":"v2"
                                                           },
                                                           "data": [
                                                             "B.${System.nanoTime()}",
                                                             "B.${System.nanoTime()}"
                                                           ]
                                                        }
                                                      }
                                                 """.stripMargin).asJsObject

}
