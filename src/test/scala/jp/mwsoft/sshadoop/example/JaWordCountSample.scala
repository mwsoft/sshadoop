package jp.mwsoft.sshadoop.example

import scala.compat.Platform.currentTime
import org.apache.hadoop.util.{ Tool, ToolRunner }
import org.apache.hadoop.conf.{ Configured, Configuration }
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapreduce.{ Job, Mapper }
import org.atilika.kuromoji.Tokenizer
import jp.mwsoft.sshadoop.{ SMapper, SReducer, SJob }

/**
 * 日本語用ワードカウント。Kuromoji利用。http://www.atilika.org/
 */
object JaWordCountSample extends Configured with Tool with App {

  exit( ToolRunner.run( this, args ) )

  val tokenizer = Tokenizer.builder().build()

  override def run( args: Array[String] ): Int = {
    val result = new SJob( getConf(), "jobName" ).
      mapper( new MyMapper() ).reducer( new MyReducer() ).combiner( new MyCombiner ).
      fileInputPath( "data/in" ).fileOutputPath( "data/out_" + currentTime ).waitForCompletion( true )
    if ( result ) 0 else 1
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

  class MyCombiner extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce( key: Text, values: Iterator[LongWritable], context: Context ) {
      context.write( key, values.map( _.get ).sum )
    }
  }

}

