import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import java.util
import org.apache.kafka.common.serialization.Serializer

class MessageSerializer extends Serializer[Message]{

  override def configure(configs: util.Map[String,_],isKey: Boolean):Unit = {

  }


  override def serialize(topic:String, data: Message):Array[Byte] = {
    try {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    }
    catch {
      case ex:Exception => throw new Exception(ex.getMessage)
    }
  }

  override def close():Unit = {

  }


}