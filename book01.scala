// Databricks notebook source
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "bfc21d23-e68e-45f2-b849-7adf3f598d1a",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="secret-scope",key="dbworkspace-secret"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/42f78800-9498-4c26-9548-8c7c50be6fea/oauth2/token")
dbutils.fs.mount(
  source = "abfss://data@datalakest1804.dfs.core.windows.net/",
  mountPoint = "/mnt/datamnt",
  extraConfigs = configs)

// COMMAND ----------

val df = spark.read.csv("/mnt/datamnt/campaign-analytics/campaignanalytics.csv")
display(df)

// COMMAND ----------

val df = spark.read.option("header", "true").csv("/mnt/datamnt/campaign-analytics/campaignanalytics.csv")
display(df)

// COMMAND ----------

//dbutils.fs.unmount("/mnt/datamnt") 

// COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

// COMMAND ----------

val textFile = spark.read.textFile("/databricks-datasets/samples/docs/README.md")


// COMMAND ----------

textFile.show()


// COMMAND ----------

// Count number or lines in textFile
textFile.count()
// Show the first line of the textFile
textFile.first()
// show all the lines with word Sudo
val linesWithSudo = textFile.filter(line => line.contains("sudo"))

// COMMAND ----------

// Output the all four lines
linesWithSudo.collect().take(4).foreach(println)
// find the lines with most words
textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)

// COMMAND ----------

val df = spark.read.json("/databricks-datasets/samples/people/people.json")

// COMMAND ----------

case class Person (name: String, age: Long)
val ds = spark.read.json("/databricks-datasets/samples/people/people.json").as[Person]

// COMMAND ----------

// define a case class that represents the device data.
case class DeviceIoTData (
  battery_level: Long,
  c02_level: Long,
  cca2: String,
  cca3: String,
  cn: String,
  device_id: Long,
  device_name: String,
  humidity: Long,
  ip: String,
  latitude: Double,
  longitude: Double,
  scale: String,
  temp: Long,
  timestamp: Long
)

val ds = spark.read.json("/databricks-datasets/iot/iot_devices.json").as[DeviceIoTData]

// COMMAND ----------

display(ds)

// COMMAND ----------

ds.describe()

// COMMAND ----------

display(ds.describe())
// or for column
display(ds.describe("c02_level"))

// COMMAND ----------

//create a variable sum_c02_1 and sum_c02_2; 
// both are correct and return same results

val sum_c02_1 = ds.select("c02_level").groupBy().sum()
val sum_c02_2 = ds.groupBy().sum("c02_level")

display(sum_c02_1)

// COMMAND ----------

ds.createOrReplaceTempView("SQL_iot_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT sum(c02_level) as Total_c02_level FROM SQL_iot_table

// COMMAND ----------

// Both will return same results
ds.select("cca2","cca3", "c02_level").show()
// or
display(ds.select("cca2","cca3","c02_level"))

// COMMAND ----------

val avg_c02 = ds.groupBy().avg("c02_level")

display(avg_c02)

// COMMAND ----------

val avg_c02_byCountry = ds.groupBy("cca3").avg("c02_level")

display(avg_c02_byCountry)

// COMMAND ----------


