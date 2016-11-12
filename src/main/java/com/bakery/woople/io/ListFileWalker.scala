package com.bakery.woople.io

import java.io.{File, FileInputStream}
import java.util.Collection

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.filefilter.IOFileFilter
import org.apache.commons.io.{DirectoryWalker, IOUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class ListFileWalker private (directoryFilter: IOFileFilter, fileFilter: IOFileFilter, depthLimit: Int)
  extends DirectoryWalker[File](directoryFilter: IOFileFilter, fileFilter: IOFileFilter, depthLimit: Int){

  override def handleFile(file: File, depth: Int, results: Collection[File]) = results.add(file)

  def list(startDirectory: File): ArrayBuffer[File] = {
    val files = new ArrayBuffer[File]
    walk(startDirectory, files)
    files
  }
}

object ListFileWalker{
  def apply(directoryFilter: IOFileFilter, fileFilter: IOFileFilter) = new ListFileWalker(directoryFilter, fileFilter, -1)

  def getMd5(path: String): String = DigestUtils.md5Hex(IOUtils.toByteArray(new FileInputStream(path)))
}
