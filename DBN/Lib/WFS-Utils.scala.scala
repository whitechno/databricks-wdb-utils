// Databricks notebook source
// 
// WFS-Utils: Whitechno File System utils
// to list and count files and directories
//

/* To run:

%run "./WFS-Utils.scala"

import com.whitechno.databricks.utils.wfs // for wfs.ls, wfs.cnt, wfs.read, wfs.touch, wfs.ppBytes

*/

// COMMAND ----------

package com.whitechno.databricks.utils

// utility case class for "Dirs and Files Count" functions
case class PathInfo(
  descr: String = "", 
  path: String, 
  name: String,
  exists: Boolean, 
  isDir: Boolean, 
  dirs_cnt: Int, 
  files_cnt: Int, 
  files_size: Long, 
  files_size_pp: String
)

object wfs { // stands for "Whitechno File System" - kinda like 'dbutils.fs'
  
  import org.apache.hadoop.fs.{FileSystem, Path}
  import org.apache.hadoop.conf.Configuration
  
  //
  // pretty print bytes
  def ppBytes(bytes: Long): String = humanReadableByteCount(bytes=bytes, si=false) 
  
  protected def humanReadableByteCount(bytes: Long, si: Boolean = false): String = {
    val unit = if(si) 1000 else 1024
    val prefix = (if(si) "kMGTPE" else "KMGTPE").split("") // need to convert to array because of possible ambiguous conversion method string2jvalue in trait Implicits of type (x: String)org.json4s.JsonAST.JValue
    val bi = if(si) "" else "i"
    
    bytes < unit match {
        case true => s"${bytes} B"
        case _ => 
          val exp = (math.log(bytes)/math.log(unit)).toInt
          val biu = bytes / math.pow(unit, exp)
          f"$biu%.2f ${prefix(exp-1)}${bi}B"
    }
  }
  
  //
  // listing files and dirs
  def ls(path: String, descr: String = ""): Seq[PathInfo] = {
    val pathPath = new Path(path)
    val fs = pathPath.getFileSystem(new Configuration)
    
    fs.exists(pathPath) match {
      
      // doesn't exist:
      case false => 
        Seq(PathInfo(descr=descr, path=pathPath.toString, name=pathPath.getName, exists=false, isDir=false, dirs_cnt=0, files_cnt=0, files_size=0L, files_size_pp=""))
      
      // exists: count files and dirs
      case true =>  fs.listStatus(pathPath).toList.map(fStat => PathInfo(
        descr = descr, 
        path = fStat.getPath.toString, 
        name = fStat.getPath.getName, 
        exists = true, 
        isDir = fStat.isDirectory, 
        dirs_cnt = if (fStat.isDirectory) 1 else 0, 
        files_cnt = if (fStat.isFile) 1 else 0, 
        files_size = fStat.getLen, 
        files_size_pp = ppBytes(fStat.getLen)
      )) 
    
    }
  }
  
  //
  // counting files and dirs
  def cnt(path: String, descr: String = ""): Seq[PathInfo] = {
    val pathPath = new Path(path)
    val fs = pathPath.getFileSystem(new Configuration)
    
    fs.exists(pathPath) match {
      
      // doesn't exist
      case false => 
        Seq(PathInfo(descr=descr, path=pathPath.toString, name=pathPath.getName, exists=false, isDir=false, dirs_cnt=0, files_cnt=0, files_size=0L, files_size_pp=""))
      
      // exists
      case true =>  
        val fStat = fs.getFileStatus(pathPath)
        val fStats = fs.listStatus(pathPath)
        Seq(PathInfo(
          descr = descr, 
          path = fStat.getPath.toString, 
          name = fStat.getPath.getName,  
          exists = true, 
          isDir = fStat.isDirectory, 
          dirs_cnt = fStats.filter(_.isDirectory).size, 
          files_cnt = fStats.filter(_.isFile).size, 
          files_size = fStats.map(_.getLen).sum, 
          files_size_pp = ppBytes(fStats.map(_.getLen).sum)
        ))
        
    }
  }
  
  // small helpful utils...
  
  //
  // get org.apache.hadoop.fs.FSDataInputStream
  def getInputStream(path: String): org.apache.hadoop.fs.FSDataInputStream = {
    val pathPath = new Path(path)
    val fs = pathPath.getFileSystem(new Configuration)
    fs.open(pathPath)
  }
  
  //
  // read text file from path and convert it to String
  // can be displayed like this: displayHTML(s"""<pre><code>${cfs.read(path)}</code></pre>""")
  // convenient for plain text and JSON files!
  // !!! not useful for XML files !!!
  def read(path: String): String = {
    val inStream = getInputStream(path)
    val out = scala.io.Source.fromInputStream(inStream).getLines.mkString("\n")
    inStream.close
    out
  }
  // for XML files the best way is to copy to /FileStore and use browser: dbutils.fs.cp(xmlFile3d, xmlFileStore)
  // or:
  // scala.xml.XML.load(getInputStream(path))
  
  //
  // touch - create "flag" file of zero size (for signaling or logs)
  def touch(path: String): Boolean = {
    val pathPath = new Path(path)
    val fs = pathPath.getFileSystem(new Configuration)
    fs.createNewFile(pathPath)
  }
  
  //
  // To get JARs on class path:
  def getJARs(): Seq[String] = {
    //import java.net.URL
    import java.net.URLClassLoader
    val cl = ClassLoader.getSystemClassLoader
    cl.asInstanceOf[URLClassLoader].getURLs.map(_.getPath).sorted.toList
  }
  
} // object wfs

