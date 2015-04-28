package cn.ac.ict.acs.netflow.load2.parquetUtil

import java.io.{FileNotFoundException, IOException}
import java.util
import javax.swing.filechooser.FileNameExtensionFilter

import cn.ac.ict.acs.netflow.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import parquet.hadoop.{ParquetFileWriter, ParquetFileReader}

import scala.collection.mutable

/**
 * Created by ayscb on 2015/4/23.
 */
object NetFlowCombineMeta extends Logging{

  private val readedFile = new mutable.HashSet[Path]

  /**
   *  merge all parquet files in to outputpath
   *
   * @param outputPath  least path
   * @param hadoopConf
   */
  def mergeParquetFiles(outputPath : Path, hadoopConf: Configuration ): Unit = {
    try {
      log.info( s"[ NetFlow : ")
      val fileSystem = outputPath.getFileSystem(hadoopConf)
      val outputStatus = fileSystem.getFileStatus(outputPath)
      val footers = ParquetFileReader.readAllFootersInParallel(hadoopConf, outputStatus)

      try {
        ParquetFileWriter.writeMetadataFile(hadoopConf, outputPath, footers)
      }
      catch  {
        case e: Exception =>
          log.warn(s"{ NetFlow : Could not write summary file for " + outputPath + " , Message: " + e.getMessage + " ]")
          val metadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_METADATA_FILE);
          if (fileSystem.exists(outputPath)) {
            fileSystem.delete(metadataPath, true);
          }
      }finally {
        fileSystem.close()
      }
    }
  }

  /**
   * 寻找 path 目录下的所有 parquet文件 （ 不包括文件夹）
   * @param fs
   * @param path
   * @return
   */
  def findNewParquetFiles( fs :FileSystem, path : Path ) : util.ArrayList[Path] = {
    val newPaths = new util.ArrayList[Path]()

    val fsList = fs.listStatus(path)
    fsList.foreach( file =>
      if( !file.isDirectory ){
        if( !readedFile.contains(file.getPath) ){
          newPaths.add(file.getPath)
          readedFile.add(file.getPath)
        }
      })

    newPaths
  }
}
