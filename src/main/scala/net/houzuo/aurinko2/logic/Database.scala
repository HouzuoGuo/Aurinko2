package net.houzuo.aurinko2.logic

import scala.collection.mutable.HashMap

class Database(val path: String) {

  private val collections = new HashMap[String, Collection]

  def create(name: String) {}
  def rename(name: String) {}
  def repair(name: String) {}
  def drop(name: String) {}
}