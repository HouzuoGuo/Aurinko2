package aurinko2.storage

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

class HashIndex(override val fc: FileChannel) extends AppendableMemoryMappedFile(fc, 67108864) {

}
