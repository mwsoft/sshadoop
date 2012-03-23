package jp.mwsoft.sshadoop.example.mapreduce.writable

import org.apache.hadoop.io._
import jp.mwsoft.sshadoop.io._
import jp.mwsoft.sshadoop.mapreduce._
import jp.mwsoft.sshadoop.util.STool

object VLongWritableExample extends STool {

  def runJob(args: Array[String]): Boolean = {

    import java.io._
    val w = new FloatWritable(1.0f)

    val os = new ByteArrayOutputStream(64)
    val out = new DataOutputStream(os)
    w.write(out)

    os.toByteArray foreach println

    true
  }

}