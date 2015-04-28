//package cn.ac.ict.acs.netflow.load2
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.Path
//
//import scala.util.Random
//
///**
// * Created by ayscb on 2015/4/17.
// */
//
//object HDFSTest{
//  def main(args: Array[String]): Unit = {
//    val hadoopConf = new Configuration()
//    hadoopConf.set("fs.defaultFS", args(0))
//
//    val path = new Path("/" + args(1))
//    val fs = path.getFileSystem(hadoopConf)
//
//    val byte = new Array[Byte]( 512 * 1024 * 1024)
//    val rd = new Random()
//    rd.nextBytes(byte)
//
//    val fin = fs.create(path,true)
//    val s = System.currentTimeMillis()
//    fin.write(byte)
//    fin.hsync()
//
//    fin.close()
//    println(System.currentTimeMillis() - s)
//  }
//}