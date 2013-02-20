package aurinko2.logic

import scala.xml.Elem

class Collection {

  // Index management
  def index(path: Seq[String]) {
  }

  def unindex(path: Seq[String]) {

  }
  // Document management
  def read(id: Int): Elem = {
    null
  }

  def insert(doc: Elem): Int = {
    0
  }

  def update(id: Int, doc: Elem): Int = {
    0
  }

  def delete(id: Int) = {

  }
}