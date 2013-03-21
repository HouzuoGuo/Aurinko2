package net.houzuo.aurinko2.test.logic

import org.scalatest.FunSuite
import TemporaryFactory.collection
import net.houzuo.aurinko2.logic.Query

class QueryTest extends FunSuite {
  val col = collection
  col.index(List("basic", "name"), 10, 10)
  col.index(List("hobby"), 10, 10)

  val docs = Array(col.insert(<root>
                                <basic><name>Howard</name></basic>
                                <hobby>
                                  <like alot="yes">sailing</like>
                                  <like verymuch="yes">programming</like>
                                </hobby>
                                <other>Aurinko2</other>
                              </root>),
    col.insert(<root><which><witch><wich>is</wich></witch></which></root>))

  test("query eq") {
    assert(new Query(<q><eq><to>Howard</to><in><path>basic</path><path>name</path></in></eq></q>, col)() == List(docs(0)))
    assert(new Query(<q><eq><to>Aurinko2</to><in><path>other</path></in></eq></q>, col)() == List(docs(0)))
    assert(new Query(<q><eq><to><witch><wich>is</wich></witch></to><in><path>which</path><path>witch</path></in></eq></q>, col)() == List(docs(1)))
  }
}