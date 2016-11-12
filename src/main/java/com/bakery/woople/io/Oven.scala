package com.bakery.woople.io

import java.io.File
import org.apache.commons.io.filefilter.{FileFilterUtils, HiddenFileFilter}
import scala.collection.mutable.{HashMap, ArrayBuffer}

object Oven {

  def main(args: Array[String]): Unit = {
    val warker = ListFileWalker(HiddenFileFilter.VISIBLE, FileFilterUtils.suffixFileFilter(".txt"))

    val result = new HashMap[String, ArrayBuffer[String]]

    warker.list(new File("/tmp")).foreach(file => {
      val path = file.getAbsolutePath
      val md5 = ListFileWalker.getMd5(path)

      if (result.contains(md5)) {
        result(md5) += path
      } else {
        result += (md5 -> ArrayBuffer(path))
      }
    })

    result.filter(_._2.size > 1).foreach(println(_))
  }
}
