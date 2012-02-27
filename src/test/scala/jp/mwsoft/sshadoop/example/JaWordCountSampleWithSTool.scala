package jp.mwsoft.sshadoop.example

import scala.compat.Platform.currentTime
import org.apache.hadoop.util.{ Tool, ToolRunner }
import org.apache.hadoop.conf.{ Configured, Configuration }
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapreduce.{ Job, Mapper }
import org.atilika.kuromoji.Tokenizer
import jp.mwsoft.sshadoop.{ SMapper, SReducer, SJob, STool }

/**
 * 日本語用ワードカウント。Kuromoji利用。http://www.atilika.org/
 */
object JaWordCountSampleWithSTool extends STool {

  val tokenizer = Tokenizer.builder().build()

  def runJob( args: Array[String] ): Boolean = {
    new SJob( getConf(), "jobName" ).
      mapper( new MyMapper() ).reducer( new MyReducer() ).
      fileInputPath( "data/in" ).fileOutputPath( "data/out_" + currentTime ).waitForCompletion( true )
  }

  class MyMapper extends SMapper[LongWritable, Text, Text, LongWritable] {
    override def map( key: LongWritable, value: Text, context: Context ) {
      tokenizer.tokenize( value ).iterator() foreach ( x => context.write( x.getSurfaceForm(), 1L ) )
    }
  }

  class MyReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce( key: Text, values: Iterator[LongWritable], context: Context ) {
      context.write( key, values.map( _.get ).sum )
    }
  }
}

