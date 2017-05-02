// Databricks notebook source
/*** WDB-Utils
Whitechno-Databricks Utilities

To run:

%run "Lib/WDB-Utils.scala"

import com.whitechno.databricks.utils.wdb.SdsUtils._

*/

// COMMAND ----------

package com.whitechno.databricks.utils.wdb
  
// ******************************************************************************
//
// Functional-stype display And StructType flattening for Spark's Datasets and DataFrames
// Functional-stype display for dbutils.fs.ls
//

// for implicit class DataFrameDisplay, table()
case class TableForDisplay(header: Seq[String], data: Seq[Seq[String]])
  
// for implicit class DbutilsDisplay, toDS()
case class FileInfoPp(path: String, name: String, size: Long)


object SdsUtils { // Spark Dataset Utils
  import org.apache.spark.sql.{DataFrame, Dataset, Column}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  
  
  implicit class DataFrameDisplay(df: DataFrame) { 
    
    // "functional-style" display for DataFrame:
    def display(): Unit = com.databricks.backend.daemon.driver.EnhancedRDDFunctions.display(df) 
    
    // collect DataFrame to TableForDisplay
    // with column names in header: Seq[String]
    // and rows in data: Seq[Seq[String]]
    // for Datasets, use it like this: sds.toDF.table
    def table(): TableForDisplay = {
      val header = df.columns.toSeq
      val data = df.collect.map(r => header.map(h => r.getAs[Any](h).toString).toSeq).toSeq
      TableForDisplay(header, data)
    }
  }
  
  // "functional-style" display for Dataset:
  implicit class DatasetDisplay[T](ds: Dataset[T]) { def display(): Unit = com.databricks.backend.daemon.driver.EnhancedRDDFunctions.display(ds) }
  
  // StructType flattening
  implicit class DataFrameFlattener(df: DataFrame) {
    def flattenSchema: DataFrame = df.select(flatten(Nil, df.schema): _*)
    
    protected def flatten(path: Seq[String], schema: DataType): Seq[Column] = schema match {
      case s: StructType => s.fields.flatMap(f => flatten(path :+ f.name, f.dataType))
      case other => col(path.map(n => s"`$n`").mkString(".")).as(path.mkString(".")) :: Nil
    }
  }
  
  
  //
  // Functional-stype display for dbutils.fs.ls:
  // - "functional-style" display for FileInfo
  // - toDS for Seq[FileInfo]
  
  implicit class DbutilsDisplay(ls: Seq[com.databricks.backend.daemon.dbutils.FileInfo]) { 
    
    def display(): Unit = com.databricks.backend.daemon.driver.EnhancedRDDFunctions.display(ls) 
    
    // In order to be able to apply .toDS, we need to import sparkSession.implicits._
    //
    //val sqlContext = SQLContext.getOrCreate(org.apache.spark.SparkContext.getOrCreate()) // < Spark 2.0
    //import sqlContext.implicits._ // < Spark 2.0
    val sparkSession = org.apache.spark.sql.SparkSession.builder.getOrCreate // >= Spark 2.0  
    import sparkSession.implicits._ // >= Spark 2.0

    def toDS(): org.apache.spark.sql.Dataset[FileInfoPp] = ls.map(fi => FileInfoPp(
      path = fi.path, 
      name = fi.name, 
      size = fi.size
    )).toDS
    
  }
} // object SdsUtils