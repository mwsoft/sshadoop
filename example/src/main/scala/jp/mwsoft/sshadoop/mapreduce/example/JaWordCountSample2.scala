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
object JaWordCountSample2 extends STool {

  def runJob( args: Array[String] ): Boolean = {
    new SJob( getConf(), "jobName" )
      .mapper( new JaWordCountMapper() )
      .reducer( new JaWordCountReducer() )
      .fileInputPaths( "data/in" )
      .fileOutputPath( "data/out/" + currentTime )
      .waitForCompletion( true )
  }

  class JaWordCountMapper extends SMapper[LongWritable, Text, Text, LongWritable] {
    val tokenizer = Tokenizer.builder().build()
    override def map( key: LongWritable, value: Text, context: Context ) {
      for ( token <- tokenizer.tokenize( value ).iterator() ) {
        val features = token.getAllFeatures()
        if ( !features.startsWith( "助詞" ) && !features.startsWith( "接続詞" ) )
          context.write( token.getSurfaceForm(), 1L )
      }
    }
  }

  class JaWordCountReducer extends SReducer[Text, LongWritable, Text, LongWritable] {
    override def reduce( key: Text, values: Iterator[LongWritable], context: Context ) {
      val sum = ( 0L /: values ) { _ + _ }
      if ( sum >= 5 ) context.write( key, sum )
    }
  }
}

