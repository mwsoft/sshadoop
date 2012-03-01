/*
 * Copyright (c) 2012 Watanabe Masato
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
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

/**
 * Hadoop Job class + "return this setter".
 * mapper method set setJarByClass/setMapOutputKeyClass/setMapOutputValueClass automatically.
 * reducer method set setOutputKeyClass/setOutputValueClass automatically.
 * mapper /reducer /combiner check input/output type inconsistence.
 *
 * @author Watanabe Masato
 */
class SJob(conf: Configuration, jobName: String) extends Job(conf, jobName) {

  def this(conf: Configuration) = this(conf, null)

  def this(jobName: String) = this(new Configuration(), jobName)

  def this() = this(new Configuration())

  protected var _mapper: Option[SMapperBase[_, _, _, _]] = None

  protected var _reducer: Option[SReducerBase[_, _, _, _]] = None

  def fileInputPath(path: Path) = { FileInputFormat.setInputPaths(this, path); this }

  def fileInputPath(path: String) = { FileInputFormat.setInputPaths(this, new Path(path)); this }

  def fileOutputPath(path: Path) = { FileOutputFormat.setOutputPath(this, path); this }

  def fileOutputPath(path: String) = { FileOutputFormat.setOutputPath(this, new Path(path)); this }

  def fileCompressOutput(value: Boolean) = { FileOutputFormat.setCompressOutput(this, true); this }

  def fileOutputCompressorClass[T <: CompressionCodec](cls: Class[T]) = {
    FileOutputFormat.setOutputCompressorClass(this, cls)
    fileCompressOutput(true)
    this
  }

  def cacheArchives(value: Boolean) = { this.setCancelDelegationTokenUponJobCompletion(value); this }

  def combinerClass[T <: Reducer[_, _, _, _]](cls: Class[T]) = { this.setReducerClass(cls); this }

  def groupingComparatorClass[T <: RawComparator[_]](cls: Class[T]) = { this.setGroupingComparatorClass(cls); this }

  def inputFormatClass[T <: InputFormat[_, _]](cls: Class[T]) = { this.setInputFormatClass(cls); this }

  def jarByClass(cls: Class[_]) = { this.setJarByClass(cls); this }

  def jobName(name: String) = { this.setJobName(name); this }

  def mapOutputKeyClass(cls: Class[_]) = { this.setMapOutputKeyClass(cls); this }

  def mapOutputValueClass(cls: Class[_]) = { this.setMapOutputValueClass(cls); this }

  def mapOutputKeyValueClass(keyCls: Class[_], valueCls: Class[_]) = {
    this.setMapOutputKeyClass(keyCls)
    this.setMapOutputValueClass(valueCls)
    this
  }

  def mapper[T <: SMapperBase[_, _, _, _]](mapperInst: T) = {
    this.setMapOutputKeyClass(mapperInst.outputKeyClass)
    this.setMapOutputValueClass(mapperInst.outputValueClass)
    if (_reducer.isEmpty) {
      this.setOutputKeyClass(mapperInst.outputKeyClass)
      this.setOutputValueClass(mapperInst.outputValueClass)
    }
    _mapper = Some(mapperInst)
    this.setJarByClass(mapperInst.getClass)
    this.setMapperClass(mapperInst.getClass)

    this
  }

  def mapperClass[T <: Mapper[_, _, _, _]](cls: Class[T], keyOutCls: Class[_] = null, valOutCls: Class[_] = null) = {
    this.setMapperClass(cls)
    if (keyOutCls != null) this.setMapOutputKeyClass(keyOutCls)
    if (valOutCls != null) this.setMapOutputValueClass(keyOutCls)
    this
  }

  def mapperManifest(mapper: Manifest[_], keyPos: Int = 2, valPos: Int = 3) {
    this.setMapOutputKeyClass(mapper.erasure)
    val list = mapper.typeArguments
    if (list.size > keyPos) this.setMapOutputKeyClass(list(keyPos).erasure)
    if (list.size > valPos) this.setMapOutputValueClass(list(valPos).erasure)
  }

  def numReduceTasks(num: Int) = { this.setNumReduceTasks(num); this }

  def outputFormatClass[T <: OutputFormat[_, _]](cls: Class[T]) = { this.setOutputFormatClass(cls); this }

  def outputKeyClass(cls: Class[_]) = { this.setOutputKeyClass(cls); this }

  def outputKeyValueClass(keyCls: Class[_], valueCls: Class[_]) = {
    this.setOutputKeyClass(keyCls)
    this.setOutputValueClass(valueCls)
    this
  }

  def outputValueClass(cls: Class[_]) = { this.setOutputValueClass(cls); this }

  def partitionerClass[T <: Partitioner[_, _]](cls: Class[T]) = { this.setPartitionerClass(cls); this }

  def reducer(reducerInst: Reducer[_, _, _, _]) = {
    if (reducerInst.isInstanceOf[SReducerBase[_, _, _, _]]) {
      val sreducerInst = reducerInst.asInstanceOf[SReducerBase[_, _, _, _]]
      this.setOutputKeyClass(sreducerInst.outputKeyClass)
      this.setOutputValueClass(sreducerInst.outputValueClass)
      _reducer = Some(sreducerInst)
    }
    this.setReducerClass(reducerInst.getClass())
    this
  }

  def reducerClass[T <: Reducer[_, _, _, _]](cls: Class[T], keyOutCls: Class[_] = null, valOutCls: Class[_] = null) = {
    this.setReducerClass(cls)
    if (keyOutCls != null) this.setOutputKeyClass(keyOutCls)
    if (valOutCls != null) this.setOutputValueClass(valOutCls)
    this
  }

  def sortComparatorClass[T <: RawComparator[_]](cls: Class[T]) = { this.setSortComparatorClass(cls); this }

  def workingDirectory(path: Path) = { this.setWorkingDirectory(path); this }

}