// Databricks notebook source
// MAGIC %md
// MAGIC ##Checking if the data was assembled and if we have access to the bronze folder

// COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/bronze"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##Reading the data in the bronze layer

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Transforming JSON fields into columns

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*","anuncio.endereco.*")
  )

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*")

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Remove columns
// MAGIC

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas","endereco")
display(df_silver)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Saving to the Silver layer

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/dados/silver/"))

// COMMAND ----------


