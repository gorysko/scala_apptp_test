package apptp.timeseries

/**
  * Created by gorysko on 9/6/17.
  */
import java.util.Date
import java.text.SimpleDateFormat

case class TimeSeriesRecord(date: Date, value: Int){
  override def toString: String = {
    val d = new SimpleDateFormat("yyyy-MM-dd").format(date.getTime)
    "%s:%d".format(d, value)
  }
}

object TimeSeriesRecord {
  def process(str: String): TimeSeriesRecord = {
    val splits = str.split(":")
    val inputDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    new TimeSeriesRecord(inputDateFormat.parse(splits(0)), splits(1).toInt)
  }
}
