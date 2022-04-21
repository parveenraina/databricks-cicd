// Databricks notebook source
val appID = "bfc21d23-e68e-45f2-b849-7adf3f598d1a"
val password = "n5d7Q~OoEQxfVSpuQONaV2dC_up8gKkzQ1HVM"
val tenantID = "42f78800-9498-4c26-9548-8c7c50be6fea"
val containerName = "data"
var storageAccountName = "datalakest1804"

val configs =  Map("fs.azure.account.auth.type" -> "OAuth",
       "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id" -> appID,
       "fs.azure.account.oauth2.client.secret" -> password,
       "fs.azure.account.oauth2.client.endpoint" -> ("https://login.microsoftonline.com/" + tenantID + "/oauth2/token"),
       "fs.azure.createRemoteFileSystemDuringInitialization"-> "true")

dbutils.fs.mount(
source = "abfss://" + containerName + "@" + storageAccountName + ".dfs.core.windows.net/",
mountPoint = "/mnt/blob-mount",
extraConfigs = configs)

// COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/blob-mount")

// COMMAND ----------

val df = spark.read.csv("/mnt/blob-mount/customer-info/customerinfo.csv")
display(df)

// COMMAND ----------

val df = spark.read.option("header", "true").csv("/mnt/blob-mount/customer-info/customerinfo.csv")
display(df)

// COMMAND ----------

val selected = df.select("UserName", "Email")
df.show(15)
df.write.csv("/mnt/blob-customer/customer-info/customerinfo-mount.csv")

// COMMAND ----------

val df1 = spark.read.csv("/mnt/blob-mount/customerinfo-mount.csv")
display(df)

// COMMAND ----------

val dfjs = spark.read.json("/mnt/blob-mount/scala.json")
display(dfjs)

// COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/blob-mount")

// COMMAND ----------

//dbutils.fs.unmount("/mnt/data") 
