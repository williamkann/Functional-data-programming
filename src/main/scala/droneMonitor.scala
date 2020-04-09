import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import org.apache.log4j.BasicConfigurator
import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import au.com.bytecode.opencsv.CSVWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object droneMonitor extends App {


  BasicConfigurator.configure()

  //props for offense

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("value.deserializer", "MessageDeserializer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "earliest")
  props.put("group.id", "consumer-group")

  val propsProducer = new Properties()
  propsProducer.put("bootstrap.servers", "localhost:9092")
  propsProducer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  propsProducer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")



  val consumer: KafkaConsumer[String, Message] = new KafkaConsumer[String, Message](props)
  consumer.subscribe(util.Arrays.asList("drone"))

  println(Console.RED + "Waiting for connections")
  println(Console.WHITE)
  while (true){
    val record = consumer.poll(1000).asScala

    for (data <- record.iterator) {
      println("Treating data\n")
      println(data)
      data.key() match {
        case "offense" => {
          (Files.exists(Paths.get("./data/nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2018.csv"))) match {
            case false => {
              println(Console.RED + "File doesn't exist, we have to create a file")
              println(Console.WHITE)
              val outputFile = new BufferedWriter(new FileWriter("./data/nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2018.csv"))
              val csvFields = Array("Summons_Number", "Plate_ID", "Registration_State", "Plate_Type", "Issue_Date", "Violation_Code", "Vehicle_Body_Type", "Vehicle_Make", "Issuing_Agency", "Street_Code1", "Street_Code2", "Street_Code3", "Vehicle_Expiration_Date", "Violation_Location", "Violation_Precinct", "Issuer_Precinct", "Issuer_Code", "Issuer_Command", "Issuer_Squad", "Violation_Time", "Street_Name", "Vehicle_Color", "Vehicle_Year")
              val csvWriter = new CSVWriter(outputFile)
              println(println(Console.RED) + "On traite un offense")
              println(Console.WHITE)


              var listOfRecords = new ListBuffer[Array[String]]()
              listOfRecords += csvFields

              listOfRecords += Array(data.value().offense.Summons_Number.toString(),
                data.value().offense.Plate_ID,
                data.value().offense.Registration_State,
                data.value().offense.Plate_Type,
                data.value().offense.Issue_Date.toString(),
                data.value().offense.Violation_Code.toString(),
                data.value().offense.Vehicle_Body_Type,
                data.value().offense.Vehicle_Make,
                data.value().offense.Issuing_Agency,
                data.value().offense.Street_Code1.toString(),
                data.value().offense.Street_Code2.toString(),
                data.value().offense.Street_Code3.toString(),
                data.value().offense.Vehicle_Expiration_Date.toString(),
                data.value().offense.Violation_Location.toString(),
                data.value().offense.Violation_Precinct.toString(),
                data.value().offense.Issuer_Precinct.toString(),
                data.value().offense.Issuer_Code.toString(),
                data.value().offense.Issuer_Command,
                data.value().offense.Issuer_Squad,
                data.value().offense.Violation_Time,
                data.value().offense.Street_Name,
                data.value().offense.Vehicle_Color,
                data.value().offense.Vehicle_Year.toString())

              csvWriter.writeAll(listOfRecords.toList)
              outputFile.close()
            }
            case true => {
              println(Console.RED + "On traite un offense")
              println(Console.WHITE)
              val outputFile = new BufferedWriter(new FileWriter("./data/nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2018.csv", true))
              val csvWriter = new CSVWriter(outputFile)

              println(data.value())
              var listOfRecords = new ListBuffer[Array[String]]()

              listOfRecords += Array(data.value().offense.Summons_Number.toString(),
                data.value().offense.Plate_ID,
                data.value().offense.Registration_State,
                data.value().offense.Plate_Type,
                data.value().offense.Issue_Date.toString(),
                data.value().offense.Violation_Code.toString(),
                data.value().offense.Vehicle_Body_Type,
                data.value().offense.Vehicle_Make,
                data.value().offense.Issuing_Agency,
                data.value().offense.Street_Code1.toString(),
                data.value().offense.Street_Code2.toString(),
                data.value().offense.Street_Code3.toString(),
                data.value().offense.Vehicle_Expiration_Date.toString(),
                data.value().offense.Violation_Location.toString(),
                data.value().offense.Violation_Precinct.toString(),
                data.value().offense.Issuer_Precinct.toString(),
                data.value().offense.Issuer_Code.toString(),
                data.value().offense.Issuer_Command,
                data.value().offense.Issuer_Squad,
                data.value().offense.Violation_Time,
                data.value().offense.Street_Name,
                data.value().offense.Vehicle_Color,
                data.value().offense.Vehicle_Year.toString())

              csvWriter.writeAll(listOfRecords.toList)
              outputFile.close()
            }
          }
        }
        case "message" => {
          (Files.exists(Paths.get("./messages.csv")) ) match {
            case false => {
              println(Console.RED + "File doesn't exist, we have to create a file")
              println(Console.WHITE)
              val outputFile = new BufferedWriter(new FileWriter("./messages.csv"))
              val csvFields = Array("location", "time", "drone id")
              val csvWriter = new CSVWriter(outputFile)
              println(Console.GREEN + "On traite un message")
              println(Console.WHITE)
              println(data.value())

              var listOfRecords = new ListBuffer[Array[String]]()
              listOfRecords += csvFields
              listOfRecords += Array(data.value().location, data.value().time.toString(), data.value().drone_id.toString())

              csvWriter.writeAll(listOfRecords.toList)
              outputFile.close()
            }
            case true => {
              println(Console.GREEN + "On traite un message")
              println(Console.WHITE)
              println(data.value())
              val outputFile = new BufferedWriter(new FileWriter("./messages.csv", true))
              val csvWriter = new CSVWriter(outputFile)
              var listOfRecords = new ListBuffer[Array[String]]()

              listOfRecords += Array(data.value().location, data.value().time.toString(), data.value().drone_id.toString())

              csvWriter.writeAll(listOfRecords.toList)
              outputFile.close()
            }
          }
        }case "error" => {
          println(Console.GREEN + "HEY ON A RECU UNE ERREUR WHALLA")
          val producer = new KafkaProducer[String, String](propsProducer)
          producer.send(new ProducerRecord[String, String]("test","control", "New York,"))
          producer.close()

        }
        case "test" => {
          println("why")
        } case default =>{
          println(default)
        }
      }
    }

  }
  print("Stop listening")

}