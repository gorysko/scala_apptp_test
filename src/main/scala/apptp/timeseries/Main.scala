package apptp.timeseries

/**
  * Created by gorysko on 9/6/17.
  */
import java.io._

import scala.collection.{AbstractIterator, Iterator}
import scala.io.Source

object Main {

  val filesLimit = 100
  val rowRegex = s"^(19\\d{2}|20\\d{2})[-](0[1-9]|1[012])[-](0[1-9]|[12][0-9]|3[01]):([0-9])+".r

  private def isValidRecord(s: String): Boolean = {
    rowRegex.pattern.matcher(s).matches
  }

  private def checkFile(name: String): Boolean = {
    val inputFile = new File(name)
    inputFile.isFile && (inputFile.getPath.endsWith(".txt") || inputFile.getPath.endsWith(".csv"))
  }

  def main(args: Array[String]) {

    if(args.length != 2) {
      println("apptopiaTest <input_file_path1>,... <output_file_path>")
      System.exit(0)
    }
    val inputFiles = args(0) split(",") take filesLimit filter checkFile
    val outputFile = new File(args(1))

    if (inputFiles.length > 0 || outputFile.exists()) {
      val sources = inputFiles map(Source
        .fromFile(_)
        .getLines
        .filter(isValidRecord)
        .map(TimeSeriesRecord process))

      val writer = new PrintWriter(outputFile)

      sources reduce merge foreach(record => writer write record.toString + "\n")
      writer.close()
    } else {
      println("You have provided no valid file or output file already exists")
      System.exit(0)
    }
  }


  def merge = (first: Iterator[TimeSeriesRecord], second: Iterator[TimeSeriesRecord]) => {
    new AbstractIterator[TimeSeriesRecord] {

      var firstBuff: Option[TimeSeriesRecord] = None
      var secondBuff: Option[TimeSeriesRecord] = None

      def hasNext: Boolean = first.hasNext || second.hasNext || firstBuff.isDefined || firstBuff.isDefined


      def next(): TimeSeriesRecord = {

        if (firstBuff.isEmpty && first.hasNext) {
          firstBuff = Some(first next)
        }

        if (secondBuff.isEmpty && second.hasNext) {
          secondBuff = Some(second next)
        }

        if (firstBuff.isDefined && secondBuff.isDefined){
          val firstRecord = firstBuff.get
          val secondRecord = secondBuff.get

          firstRecord.date.compareTo(secondRecord date) match {
            case 0 =>
              firstBuff = None
              secondBuff = None
              firstRecord.copy(value = firstRecord.value + secondRecord.value)
            case 1 =>
              secondBuff = None
              secondRecord
            case -1 =>
              firstBuff = None
              firstRecord
          }
        } else {
          firstBuff match {
            case Some(value) =>
              firstBuff = None
              value
            case None =>
              val secondRecord = secondBuff.get
              secondBuff = None
              secondRecord
          }
        }
      }
    }
  }
}