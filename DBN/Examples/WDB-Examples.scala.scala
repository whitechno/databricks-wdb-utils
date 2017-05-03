// Databricks notebook source
// MAGIC %run ../Lib/WDB-Setup.scala

// COMMAND ----------



// COMMAND ----------

wfs.cnt("/FileStore").toDS.display

// COMMAND ----------

wfs.cnt("/FileStore/40bdfa65-eca8-40eb-ad45-060c0a227c9d.png").toDS.display

// COMMAND ----------

wfs.cnt("/FileStore/40bdfa65-eca8-40eb-ad45-060c0a227c9d.pn").toDS.display

// COMMAND ----------

wfs.ls("/FileStore/40bdfa65-eca8-40eb-ad45-060c0a227c9d.png").toDS.display

// COMMAND ----------

wfs.ls("/FileStore").toDS.where("name NOT LIKE '%.png'").display

// COMMAND ----------

wfs.ls("/FileStore").filter(_.isDir).toDS.display

// COMMAND ----------

val dirs = wfs.ls("/FileStore").filter(_.isDir).map(_.path)

// COMMAND ----------

dirs.flatMap(dir => wfs.cnt(dir)).toDS.display

// COMMAND ----------

sc.parallelize(dirs).flatMap(dir => wfs.cnt(dir)).toDS.display

// COMMAND ----------

