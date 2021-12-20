import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.desc

object CaseClassEx extends App{  
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("CaseclassEx")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  
  /*val schema1 = StructType(Array(StructField("device_id",StringType,true),
  StructField("cca2", StringType, true),
  StructField("c02_level", LongType, true),
  StructField("cca3", StringType, true),
  StructField("cn", StringType, true),
  StructField("device_id", LongType, true),
  StructField("humidity", LongType, true),
  StructField("ip", StringType, true),
  StructField("latitude", DoubleType, true),
  StructField("lcd", StringType, true),
  StructField("longitude", DoubleType, true),
  StructField("scale", StringType, true),
  StructField("temp", LongType, true),
  StructField("timestamp", LongType,true),
  StructField("battery_level", LongType, true)  
  ))*/
  
  //read json file through case class 
  import spark.implicits._  
  val ds = spark.read.json("/home/han/LNSparkCaseClassEx/iot_devices.json").as[DeviceIoTData]

  
  //ds.show(10)
  //println(ds.printSchema())
  //DataSet Operations
  val fileterTempDS = ds.filter(({d => {d.temp > 30 && d.humidity > 70}}))
  fileterTempDS.orderBy(desc("humidity")).show(10)
  
  //another example that results in another, smaller Dataset
  val dsTemp = ds.filter(d => {d.temp > 25})                        
    .map(d => (d.device_name, d.temp, d.device_id, d.cca3))         
    .toDF("device_name", "Temp", "device_id", "cca3")
    .as[DeviceTempByCountry]

  //do the same thing by using this 
  //Semantically,  select()  is  like  map()  in  the  previous  query,
  val dsTemp2= ds.select($"device_name", $"temp", $"device_id", $"cca3")
    .where("temp > 25")
    .as[DeviceTempByCountry]
  dsTemp.show(5)
  println("--------------------------change Line-------------------------------")
  
  dsTemp2.show(5)
  val firstRow = dsTemp.first()
  println(firstRow)
  
  









  spark.stop()

}
