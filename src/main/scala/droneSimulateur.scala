import java.util
import java.util.Properties
import java.util.Calendar
import scala.collection.JavaConverters._

import scala.util.control.Breaks._
import droneMonitor.{consumer, props}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.BasicConfigurator


object droneSimulateur extends App {

  BasicConfigurator.configure()
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("value.serializer", "MessageSerializer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val propsReceiver = new Properties()
  propsReceiver.put("bootstrap.servers", "localhost:9092")
  propsReceiver.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  propsReceiver.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  propsReceiver.put("auto.offset.reset", "earliest")
  propsReceiver.put("group.id", "consumer-group")



  val producer = new KafkaProducer[String, Message](props)
  val offense1 = new ProducerRecord[String, Message]("drone", "offense", new Message("New York", Calendar.getInstance().getTime,1, new Offense(509246,"GZH7067","NY","PAS",Calendar.getInstance().getTime,7,"SUBN","TOYOT","V",10610,34330,34350,20180630,14443,14,14,359594,"T102","J","1120A","W 175 ST","WHITE",2014)))
  val offense2 = new ProducerRecord[String, Message]("drone", "offense", new Message("New York", Calendar.getInstance().getTime,1,new Offense(351658,"GZH7067","NY","PAS", Calendar.getInstance().getTime, 7, "SUBN","OPEL","V",10510,34310,34330,20170228,12301,13,13,364832,"T102","M","0555P","W 176 ST","RED",2011)))
  val offense3 = new ProducerRecord[String, Message]("drone", "offense", new Message("New York", Calendar.getInstance().getTime,1,  new Offense(400626, "FZX9232","NY","PAS",Calendar.getInstance().getTime,5,"SUBN","FORD","V",54070, 39430,54930,20161130,12334,10,10,960290,"T102","M","00555P","N.PORTLAND AVE","BLUE", 2013)))
  val message = new ProducerRecord[String, Message]("drone","message", new Message("New York", Calendar.getInstance().getTime,1, null))
  val r = scala.util.Random
  while (true) {

    r.nextInt(4) match {
      case 1 => {
        producer.send(message)
        producer.send(offense1)
        Thread.sleep(10000)
      }
      case 0 => {

        producer.send(message)
        val time1 = r.nextInt(10000)
        Thread.sleep(time1)

        val time2 = r.nextInt(10000 - time1)
        Thread.sleep(time2)
        producer.send(offense1)
        println(offense1.value().offense.Summons_Number)
        println("Sending Offense1")

        val time3 = r.nextInt(10000 - time1 - time2)
        Thread.sleep(time3)
        producer.send(offense2)
        println(offense2.value().offense.Summons_Number)
        println("Sending Offense2")
      }
      case 2 => {
        producer.send(message)
        val time1 = r.nextInt(10000)
        Thread.sleep(time1)

        val time2 = r.nextInt(10000 - time1)
        Thread.sleep(time2)
        producer.send(offense3)
        println(offense3.value().offense.Summons_Number)
        println("Sending Offense3")

      }
      case 3 => {
        println(Console.YELLOW + "sending test")
        val message2 = new ProducerRecord[String, Message]("drone","error", new Message("New York", Calendar.getInstance().getTime,1, null))

        val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](propsReceiver)
        //consumer.subscribe(util.Arrays.asList("scala"))
        consumer.subscribe(util.Arrays.asList("test"))

        producer.send(message2)
        println(Console.YELLOW + "error send")
        breakable {
          while (true) {
            println("waiting")
            val record = consumer.poll(1000).asScala
            if (!record.isEmpty) {
              println("data receive")
              val data = record.iterator.next()
              if (data != null) {
                println(Console.GREEN + "treating the data")
                if (data.key() == "control") {
                  println("the monitor has controled the drone")
                  break
                }
              }
            }
          }
        }
        println("the error have been managed")
        consumer.close()
      }

    }
  }


  //producer.send(new ProducerRecord[String, String]("test","test", "New York," ))

  producer.close()

  println(Console.YELLOW + "exiting the program")




  /**
   * Properties used for messages.csv
   * Location
   * Time
   * Drone ID
   */


  /**
   *
   * Properties used for Parking_Violations_Issued_-_Fiscal_Year_2018.csv
   * Summons_Number
   * Plate_ID
   * Registration_State
   * Plate_Type
   * Issue_Date
   * Violation_Code
   * Vehicle_Body_Type
   * Vehicle_Make
   * Issuing_Agency
   * Street_Code1
   * Street_Code2
   * Street_Code3
   * Vehicle_Expiration_Date
   * Violation_Location
   * Violation_Precinct
   * Issuer_Precinct
   * Issuer_Code
   * Issuer_Command
   * Issuer_Squad
   * Violation_Time
   * Street_Name
   * Vehicle_Color
   * Vehicle_Year
   */
}
