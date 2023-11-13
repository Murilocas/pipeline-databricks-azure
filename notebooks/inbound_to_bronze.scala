// Databricks notebook source
// DBTITLE 1,Checking if the data was assembled and if we have access to the inbound folder
display(dbutils.fs.ls("/mnt/dados/inbound"))

// COMMAND ----------

// DBTITLE 1,Reading data in the inbound layer
val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// DBTITLE 1,Remove columns
val dados_anuncio = dados.drop("imagens","usuario")
display(dados_anuncio)

// COMMAND ----------

// DBTITLE 1,Creating an identification column
import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// DBTITLE 1,Saving in bronze layer
val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/bronze"))

// COMMAND ----------


