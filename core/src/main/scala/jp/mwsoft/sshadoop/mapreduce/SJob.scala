/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.mwsoft.sshadoop.mapreduce

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Reducer
import java.net.URI
import org.apache.hadoop.io.RawComparator
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{ BooleanWritable, ByteWritable, IntWritable, LongWritable, FloatWritable, DoubleWritable }
import org.apache.hadoop.io.{ Text, BytesWritable, ArrayWritable }
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import jp.mwsoft.sshadoop.util.ImplicitConversions

/**
 * Hadoop Job class + "return this setter".
 * mapper method set setJarByClass/setMapOutputKeyClass/setMapOutputValueClass automatically.
 * reducer method set setOutputKeyClass/setOutputValueClass automatically.
 * mapper /reducer /combiner check input/output type inconsistence.
 *
 * @author Watanabe Masato
 */
class SJob( conf: Configuration, jobName: String )
    extends Job( conf, jobName ) with ImplicitConversions {

  def this( conf: Configuration ) = this( conf, null )

  def this( jobName: String ) = this( new Configuration(), jobName )

  def this() = this( new Configuration() )

  /** FileInputFormat.setInputPaths */
  def fileInputPaths( paths: Path* ) = { FileInputFormat.setInputPaths( this, paths: _* ); this }

  /** FileInputFormat.setInputPaths */
  def fileInputPaths( path: String ) = { FileInputFormat.setInputPaths( this, path ); this }

  /** FileInputFormat.setInputPathFilter */
  def fileInputPathFilter[T <: PathFilter]( filter: Class[T] ) = {
    FileInputFormat.setInputPathFilter( this, filter )
    this
  }

  /** FileOutputFormat.setOutputPaths */
  def fileOutputPath( path: Path ) = { FileOutputFormat.setOutputPath( this, path ); this }

  /** FileOutputFormat.setOutputPaths */
  def fileOutputPath( path: String ) = { FileOutputFormat.setOutputPath( this, new Path( path ) ); this }

  /** FileOutputFormat.setCompressOutput */
  def fileCompressOutput( value: Boolean ) = { FileOutputFormat.setCompressOutput( this, true ); this }

  /** FileOutputFormat.setOutputCompressorClass */
  def fileOutputCompressorClass[T <: CompressionCodec]( cls: Class[T] ) = {
    FileOutputFormat.setOutputCompressorClass( this, cls )
    this
  }

  /** SequenceFileOutputFormat.setOutputCompressionType */
  def outputCompressionType( style: SequenceFile.CompressionType ) = {
    SequenceFileOutputFormat.setOutputCompressionType( this, style )
    this
  }

  /** Job.setCancelDelegationTokenUponJobCompletion */
  def cancelDelegationTokenUponJobCompletion( value: Boolean ) = {
    this.setCancelDelegationTokenUponJobCompletion( value )
    this
  }

  /** Job.setCombinerClass */
  def combiner[T <: Reducer[_, _, _, _]]( combiner: T ) = { this.setCombinerClass( combiner.getClass() ); this }

  /** Job.setCombinerClass */
  def combinerClass[T <: Reducer[_, _, _, _]]( cls: Class[T] ) = { this.setCombinerClass( cls ); this }

  /** Job.setGroupingComparatorClass */
  def groupingComparatorClass[T <: RawComparator[_]]( cls: Class[T] ) = { this.setGroupingComparatorClass( cls ); this }

  /** Job.setInputFormatClass */
  def inputFormatClass[T <: InputFormat[_, _]]( cls: Class[T] ) = { this.setInputFormatClass( cls ); this }

  /** Job.setJarByClass */
  def jarByClass( cls: Class[_] ) = { this.setJarByClass( cls ); this }

  /** Job.setJobName */
  def jobName( name: String ) = { this.setJobName( name ); this }

  /** Job.setMapOutputKeyClass */
  def mapOutputKeyClass( cls: Class[_] ) = { this.setMapOutputKeyClass( cls ); this }

  /** Job.setMapOutputValueClass */
  def mapOutputValueClass( cls: Class[_] ) = { this.setMapOutputValueClass( cls ); this }

  /**
   * setMapOutputKeyClass( keyCls )
   * setMapOutputValueClass( valueCls )
   */
  def mapOutputKeyValueClass( keyCls: Class[_], valueCls: Class[_] ) = {
    this.setMapOutputKeyClass( keyCls )
    this.setMapOutputValueClass( valueCls )
    this
  }

  /**
   * Job.setJarByClass( inst.getClass )
   * Job.setMapperClass( inst.getClass )
   * If instance isInstanceOf SMapper, call setMapOutputKeyClass and setMapOutputValueClass automatically.
   */
  def mapper[T <: Mapper[_, _, _, _]]( inst: T ) = {
    if ( inst.isInstanceOf[SMapper[_, _, _, _]] ) {
      val sinst = inst.asInstanceOf[SMapper[_, _, _, _]]
      this.setMapOutputKeyClass( sinst.outputKeyClass )
      this.setMapOutputValueClass( sinst.outputValueClass )
    }
    this.setJarByClass( inst.getClass )
    this.setMapperClass( inst.getClass )
    this
  }

  /** Job.setMapperClass */
  def mapperClass[T <: Mapper[_, _, _, _]]( cls: Class[T], keyOutCls: Class[_] = null, valOutCls: Class[_] = null ) = {
    this.setMapperClass( cls )
    if ( keyOutCls != null ) this.setMapOutputKeyClass( keyOutCls )
    if ( valOutCls != null ) this.setMapOutputValueClass( keyOutCls )
    this
  }

  /** Job.setNumReduceTasks */
  def numReduceTasks( num: Int ) = { this.setNumReduceTasks( num ); this }

  /** Job.setOutputFormatClass */
  def outputFormatClass[T <: OutputFormat[_, _]]( cls: Class[T] ) = { this.setOutputFormatClass( cls ); this }

  /** Job.setOutputKeyClass */
  def outputKeyClass( cls: Class[_] ) = { this.setOutputKeyClass( cls ); this }

  /**
   * Job.setOutputKeyClass( keyCls )
   * Job.setOutputValueClass( valueCls )
   */
  def outputKeyValueClass( keyCls: Class[_], valueCls: Class[_] ) = {
    this.setOutputKeyClass( keyCls )
    this.setOutputValueClass( valueCls )
    this
  }

  /** Job.setOutputValueClass */
  def outputValueClass( cls: Class[_] ) = { this.setOutputValueClass( cls ); this }

  /** Job.setPartitionerClass */
  def partitionerClass[T <: Partitioner[_, _]]( cls: Class[T] ) = { this.setPartitionerClass( cls ); this }

  /**
   * Job.setReducerClass( inst.getClass )
   * If instance isInstanceOf SReducer, call setOutputKeyClass and setOutputValueClass automatically.
   */
  def reducer( inst: Reducer[_, _, _, _] ) = {
    if ( inst.isInstanceOf[SReducer[_, _, _, _]] ) {
      val sInst = inst.asInstanceOf[SReducer[_, _, _, _]]
      this.setOutputKeyClass( sInst.outputKeyClass )
      this.setOutputValueClass( sInst.outputValueClass )
    }
    this.setReducerClass( inst.getClass() )
    this
  }

  /** Job.setReducerClass */
  def reducerClass[T <: Reducer[_, _, _, _]]( cls: Class[T], keyOutCls: Class[_] = null, valOutCls: Class[_] = null ) = {
    this.setReducerClass( cls )
    if ( keyOutCls != null ) this.setOutputKeyClass( keyOutCls )
    if ( valOutCls != null ) this.setOutputValueClass( valOutCls )
    this
  }

  /** Job.sortComparatorClass */
  def sortComparatorClass[T <: RawComparator[_]]( cls: Class[T] ) = { this.setSortComparatorClass( cls ); this }

  /** Job.setWorkingDirectory */
  def workingDirectory( path: Path ) = { this.setWorkingDirectory( path ); this }

}