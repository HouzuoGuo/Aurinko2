package net.houzuo.aurinko2.logic

import scala.xml.Node

class Query(val query: Node, val col: Collection, val result: List[Int]) {
  for (op <- query.child) {
    op.label match {
      /*
       * <eq limit="1" skip="1">
       *   <to>
       *     <performance>100%</performance>
       *   </to>
       *   <in>
       *     <path>sales</path>
       *     <path>product1</path>
       *   </in>
       * </eq>
       */
      case "eq" =>
        val children = op.child.map { c => c.label -> c } toMap

        val limit = op.attribute("limit") match {
          case Some(number) => number.text toInt
          case None         => -1
        }

        val skip = op.attribute("limit") match {
          case Some(number) => number.text.toInt
          case None         => -1
        }
      case _ => throw new Exception(s"Unknown query operation ${op.label}")
    }
  }
}