package cn.ac.ict.acs.netflow.load2.parquetUtil

import cn.ac.ict.acs.netflow.load2.parquetUtil.parquetState.parquetState
import cn.ac.ict.acs.netflow.{NetFlowConf, Logging}
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path}
import parquet.hadoop.{ParquetFileReader, ParquetFileWriter}

import scala.collection.mutable

/**
 * Created by ayscb on 2015/4/23.
 */
object NetFlowCombineMeta extends Logging{

  private val META_FILE = ParquetFileWriter.PARQUET_METADATA_FILE
  private val COMMON_META_FILE = ParquetFileWriter.PARQUET_COMMON_METADATA_FILE
  private val SUCCESS_FIME = "_SUCCESS"

  private val TEMP_DICT = "_temporary"
  private val readedFile = new mutable.HashSet[Path]

  /**
   *  merge all parquet files in to outputpath
   *
   * @param outputPath  least path
   * @param conf
   */
  private def mergeParquetFiles(outputPath : Path, conf: NetFlowConf ) : parquetState  = {
    try {
      log.info( s"[ NetFlow : ")
      val fileSystem = outputPath.getFileSystem(conf.hadoopConfigure)
      val outputStatus = fileSystem.getFileStatus(outputPath)
      val footers = ParquetFileReader.readAllFootersInParallel(conf.hadoopConfigure, outputStatus)

      try {
        ParquetFileWriter.writeMetadataFile(conf.hadoopConfigure, outputPath, footers)

        //delete the _temporary dic
        val tempFileNum = fileSystem.listStatus( new Path ( outputPath, TEMP_DICT)).length
        require(tempFileNum == 0 ,
          "The %s /_temporary should be empty, but now it has %d elements. "
            .format(outputPath.toUri.toString, tempFileNum))

        logInfo("[ Parquet ] Combine %s's parquets finished, Delete the %s ".
          format(outputPath.toUri.toString,TEMP_DICT))
        fileSystem.delete( new Path( outputPath, TEMP_DICT), true)

        // write _success File
        fileSystem.create(new Path(outputPath,SUCCESS_FIME)).close()

        fileSystem.close()
        logInfo("[ Parquet ] Write _sunncess file ")
        parquetState.FINISH
      }
      catch  {
        case e: Exception =>
          log.warn(s"{ NetFlow : Could not write summary file for " + outputPath + " , Message: " + e.getMessage + " ]")
          val metadataPath = new Path(outputPath, META_FILE);
          if (fileSystem.exists(outputPath)) {
            fileSystem.delete(metadataPath, true)
          }
          parquetState.FAIL
      }finally {
        fileSystem.close()
      }
    }
  }

//  /**
//   * 寻找 path 目录下的所有 parquet文件 （ 不包括文件夹）
//   * @param fs
//   * @param path
//   * @return
//   */
//  def findNewParquetFiles( fs :FileSystem, path : Path ) : util.ArrayList[Path] = {
//    val newPaths = new util.ArrayList[Path]()
//
//    val fsList = fs.listStatus(path)
//    fsList.foreach( file =>
//      if( !file.isDirectory ){
//        if( !readedFile.contains(file.getPath) ){
//          newPaths.add(file.getPath)
//          readedFile.add(file.getPath)
//        }
//      })
//
//    newPaths
//  }

  /**
   * only all sub parquet files are ready that
   * we can combine the parquet file
   * @param fs
   * @param path
   * @return
   */
  def combineFiles(fs :FileSystem, path : Path, conf : NetFlowConf) : parquetState  = {
    if( !fs.exists(path)){
      logError("[ parquet ] The path %s does not exist in Hadoop ".format(path.toUri.toString) )
      return parquetState.DIC_NOT_EXIST
    }

    if( !fs.isDirectory(path) ){
      logError("[ parquet ] The path %s should be dictionary in Hadoop ".format(path.toUri.toString) )
      return parquetState.NO_DIC
    }

    val fsList: Array[FileStatus] = fs.listStatus(path)
    if( fsList.length == 0)  return parquetState.DIC_EMPTY

    val filterFiles: Array[FileStatus] = fsList.filter(
      filter=>filter.getPath.getName.startsWith("_"))

    filterFiles.size match {
      case 0 =>
        logError("[ parquet ] The path %s is not a parquet dictionary. ".format(path.getName))
        parquetState.NO_PARQUET

      case 1 =>   // only _temp [D]
        logInfo("[ parquet ] we will combine all parquet' files only when there is empty in path %s "
          .format(filterFiles(0).getPath.getName) )
        val tempFile = fs.listStatus(filterFiles(0).getPath)
        if( tempFile.size != 0 )
          parquetState.UNREADY
        else
          mergeParquetFiles(path,conf)

      case 2 | 3 =>
        filterFiles.foreach( file => {
          val name = file.getPath.getName
          if( name.equalsIgnoreCase(META_FILE) || name.equalsIgnoreCase(COMMON_META_FILE) )
            fs.delete(file.getPath,true)
        })

        val tempFilePath = filterFiles.find( file => file.getPath.getName.equalsIgnoreCase(TEMP_DICT) ).get.getPath
        val tempFile = fs.listStatus(tempFilePath)
        if( tempFile.size != 0 )
          parquetState.UNREADY
        else
          mergeParquetFiles(path,conf)

      case 4 =>  parquetState.FINISH
      case _ =>
        logError("[ parquet ] TOO many '_xxx' files in path %s ".format(filterFiles.head.getPath.getParent.toString))
        parquetState.STRUCT_ERROR
    }
  }
}

object parquetState extends Enumeration{
  type parquetState = Value
  val DIC_NOT_EXIST,NO_DIC,DIC_EMPTY,NO_PARQUET,UNREADY,FINISH,FAIL,STRUCT_ERROR = Value
}
