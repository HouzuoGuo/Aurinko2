package aurinko2.test.logic

import java.nio.file.Files
import java.io.File
import aurinko2.logic.Collection

object TemporaryFactory {
  def collection = {
    val dir = Files.createTempDirectory(null)
    new File(dir.toString()).deleteOnExit()
    new Collection(dir.toString())
  }
}