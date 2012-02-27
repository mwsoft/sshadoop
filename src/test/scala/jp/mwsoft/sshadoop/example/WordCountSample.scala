package jp.mwsoft.sshadoop

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.{ Configured, Configuration }
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Mapper
import org.atilika.kuromoji.Tokenizer

object WordCountSample extends Configured with Tool with App {

  run( args )

  override def run( args: Array[String] ): Int = {

    val job = new SJob( getConf(), "jobName" )

    job mapper ( new MyMapper() ) reducer ( new MyReducer() ) combiner ( new MyReducer() )

    if ( job.waitForCompletion( true ) ) 0 else 1
  }

  class MyMapper extends SMapper[LongWritable, Text, Text, LongWritable] {
    val tokenizer = Tokenizer.builder().build()
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

