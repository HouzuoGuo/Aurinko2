package aurinko2.storage

import java.nio.MappedByteBuffer

class Log(val buf: MappedByteBuffer) {
  def append(data: String) {}
}
