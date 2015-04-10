package cn.ac.ict.acs.netflow.util

import java.io.{BufferedWriter, FileWriter, File}

/**
 * write the result to the file
 * Created by ayscb on 2015/4/7.
 */
object FileUtil {
 // def apply(path :String) = new FileUtil(path)
}

case class FileUtil( path : String) {

  val file = new File(path)
  if( file.exists() )
    throw new IllegalArgumentException("the file has already exist!")

  @transient var fw = new FileWriter(file)
  @transient var bw = new BufferedWriter(fw)

  def writeText( value : String) = {
    if( fw == null ){
      fw = new FileWriter(file)
      bw = new BufferedWriter(fw)
    }
    bw.write(value)
    bw.newLine()
  }

  def close() = {
    bw.close()
    fw.close()
  }
}