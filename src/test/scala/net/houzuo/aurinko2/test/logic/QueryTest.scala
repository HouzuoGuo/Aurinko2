package net.houzuo.aurinko2.test.logic

import org.scalatest.FunSuite

import TemporaryFactory.collection
import net.houzuo.aurinko2.logic.Query

class QueryTest extends FunSuite {
  val col = collection
  col.index(List("basic", "name"), 10, 10)
  col.index(List("hobby", "like"), 10, 10)

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
                   <like alot="yes">SAILING</like>
                   <like verymuch="yes">PROGRAMMING</like>
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
    assert(new Query(col).qeq(<eq skip="0">
                                <to><name>Howard</name></to>
                                <in><path>basic</path><path>name</path></in>
                              </eq>) == List(docs(0), docs(1), docs(2)))
    assert(new Query(col).qeq(<eq skip="1" limit="1">
                                <to><other>Aurinko2</other></to>
                                <in><path>other</path></in>
                              </eq>) == List(docs(1)))
    assert(new Query(col).qeq(<eq>
                                <to><like alot="yes">sailing</like></to>
                                <in><path>hobby</path><path>like</path></in>
                              </eq>) == List(docs(0), docs(2)))
    assert(new Query(col).qeq(<eq>
                                <to><like>sailing</like></to><in><path>hobby</path><path>like</path></in>
                              </eq>) == List())
    assert(new Query(col).qeq(<eq limit="1">
                                <to><witch><wich>is</wich></witch></to>
                                <in><path>which</path><path>witch</path></in>
                              </eq>) == List(docs(3)))
  }

  test("has") {
    assert(new Query(col).qhas(<has><path>basic</path></has>) == List(docs(0), docs(1), docs(2)))
    assert(new Query(col).qhas(<has skip="1" limit="1"><path>basic</path></has>) == List(docs(1)))
    assert(new Query(col).qhas(<has><path>asdf</path></has>) == List())
    assert(new Query(col).qhas(<has limit="2"><path>basic</path></has>) == List(docs(0), docs(1)))
  }

  test("all") {
    assert(new Query(col).eval(<q><all/></q>) == Set(docs(0), docs(1), docs(2), docs(3)))
  }

  test("diff") {
    assert(new Query(col).eval(<q>
                                 <diff>
                                   <all/>
                                   <eq><to><like alot="yes">sailing</like></to><in><path>hobby</path><path>like</path></in></eq>
                                   <eq limit="1"><to><witch><wich>is</wich></witch></to><in><path>which</path><path>witch</path></in></eq>
                                 </diff>
                               </q>) == Set(docs(1)))
  }

  test("intersect") {
    assert(new Query(col).eval(<q>
                                 <intersect>
                                   <diff>
                                     <eq>
                                       <to><name>Howard</name></to>
                                       <in><path>basic</path><path>name</path></in>
                                     </eq>
                                     <all/>
                                   </diff>
                                   <eq><to><like alot="yes">sailing</like></to><in><path>hobby</path><path>like</path></in></eq>
                                 </intersect>
                               </q>) == Set(docs(0), docs(2)))
  }

  test("union") {
    assert(new Query(col).eval(<q>
                                 <eq><to><like alot="yes">sailing</like></to><in><path>hobby</path><path>like</path></in></eq>
                                 <eq limit="1"><to><witch><wich>is</wich></witch></to><in><path>which</path><path>witch</path></in></eq>
                               </q>) == Set(docs(0), docs(2), docs(3)))
  }
}
