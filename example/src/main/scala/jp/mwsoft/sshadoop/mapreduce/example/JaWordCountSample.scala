package jp.mwsoft.sshadoop.mapreduce.example

import scala.compat.Platform.currentTime
import org.apache.hadoop.io.{ Text, LongWritable }
import org.atilika.kuromoji.Tokenizer
import jp.mwsoft.sshadoop.mapreduce._
import jp.mwsoft.sshadoop.util.STool

/**
 * 日本語用ワードカウント。Kuromoji利用
 * http://www.atilika.org/
 */
object JaWordCountSample extends STool {

  def runJob( args: Array[String] ): Boolean = {
    new SJob( getConf(), "jobName" )
      .mapper( new JaWordCountMapper() )
      .reducer( new JaWordCountReducer() )
      .fileInputPaths( "data/in" )
      .fileOutputPath( "data/out/" + currentTime )
      .waitForCompletion( true )
  }

  class JaWordCountMapper extends SMapper[LongWritable, Text, Text, LongWritable] {
    val tokenizer = Tokenizer.builder().userDictionary(getClass.getResourceAsStream("user.dic")).build()
    override def map( key: LongWritable, value: Text, context: Context ) {
      tokenizer.tokenize( value ).iterator() foreach ( x => context.write( x.getSurfaceForm(), 1L ) )
    }
  }

  class JaWordCountReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce( key: Text, values: Iterator[LongWritable], context: Context ) {
      context.write( key, ( 0L /: values ) { _ + _ } )
    }
  }
}

