package net.houzuo.aurinko2.test.logic

import org.scalatest.FunSuite
import TemporaryFactory.collection
import net.houzuo.aurinko2.logic.Query

class QueryTest extends FunSuite {
  val col = collection
  col.index(List("basic", "name"), 10, 10)
  col.index(List("hobby"), 10, 10)

  val docs = Array(
    col.insert(<root>
                 <basic><name>Howard</name></basic>
                 <hobby>
                   <like alot="yes">sailing</like>
                   <like verymuch="yes">programming</like>
                 </hobby>
                 <other>Aurinko2</other>
               </root>),
    col.insert(<root>
                 <basic><name>Howard</name></basic>
                 <hobby>
                   <like alot="yes">sailing</like>
                   <like verymuch="yes">programming</like>
                 </hobby>
                 <other>Aurinko2</other>
               </root>),
    col.insert(<root>
                 <basic><name>Howard</name></basic>
                 <hobby>
                   <like alot="yes">sailing</like>
                   <like verymuch="yes">programming</like>
                 </hobby>
                 <other>Aurinko2</other>
               </root>),
    col.insert(<root><which><witch><wich>is</wich></witch></which></root>))

  test("eq") {
    assert(new Query(col).qeq(<eq skip="0"><to><name>Howard</name></to><in><path>basic</path><path>name</path></in></eq>) == List(docs(0), docs(1), docs(2)))
    assert(new Query(col).qeq(<eq skip="1" limit="1"><to><other>Aurinko2</other></to><in><path>other</path></in></eq>) == List(docs(1)))
    println(new Query(col).qeq(<eq limit="1"><to><witch><wich>is</wich></witch></to><in><path>which</path><path>witch</path></in></eq>))
    assert(new Query(col).qeq(<eq limit="1"><to><witch><wich>is</wich></witch></to><in><path>which</path><path>witch</path></in></eq>) == List(docs(3)))
  }
}
