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

class SJob(conf: Configuration, jobName: String) extends Job(conf, jobName) {

  def this(conf: Configuration) = this(conf, null)

  def this(jobName: String) = this(new Configuration(), jobName)

  def this() = this(new Configuration())

  protected var _mapper: Option[SMapper[_, _, _, _]] = None

  protected var _reducer: Option[SReducer[_, _, _, _]] = None

  protected var _combiner: Option[SReducer[_, _, _, _]] = None

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

  def combiner(combinerInst: SReducer[_, _, _, _]) = {
    if (_mapper.isDefined) {
      if (_mapper.get.outputKeyClass != combinerInst.inputKeyClass)
        throw new IllegalArgumentException("combiner input key must be equal to mapper output key")
      if (_mapper.get.outputValueClass != combinerInst.inputValueClass)
        throw new IllegalArgumentException("combiner value key must be equal to mapper output value")
    }
    if (_reducer.isDefined) {
      if (_reducer.get.inputKeyClass != combinerInst.outputKeyClass)
        throw new IllegalArgumentException("combiner output key must be equal to mapper input key")
      if (_reducer.get.inputValueClass != combinerInst.outputValueClass)
        throw new IllegalArgumentException("combiner output value must be equal to mapper input value")
    }
    _combiner = Some(combinerInst)
    this.setCombinerClass(combinerInst.getClass())
    this
  }

  def jCombinerr[T <: Reducer[_, _, _, _]](cls: Class[T]) = {
    this.setReducerClass(cls)
    this
  }

  def groupingComparatorClass[T <: RawComparator[_]](cls: Class[T]) = { this.setGroupingComparatorClass(cls); this }

  def inputFormatClass[T <: InputFormat[_, _]](cls: Class[T]) = { this.setInputFormatClass(cls); this }

  def jarByClass(cls: Class[_]) = { this.setJarByClass(cls); this }

  def jobName(name: String) = { this.setJobName(name); this }

  def mapOutputKeyClass(cls: Class[_]) = { this.setMapOutputKeyClass(cls); this }

  def mapOutputValueClass(cls: Class[_]) = { this.setMapOutputValueClass(cls); this }

  def mapper[T <: SMapper[_, _, _, _]](mapperInst: T) = {
    if (_combiner.isDefined) {
      if (_combiner.get.inputKeyClass != mapperInst.outputKeyClass)
        throw new IllegalArgumentException("mapper output key must be equal to combiner input key")
      if (_combiner.get.inputValueClass != mapperInst.outputValueClass)
        throw new IllegalArgumentException("mapper output value must be equal to combiner input value")
    }
    if (_reducer.isDefined) {
      if (_reducer.get.inputKeyClass != mapperInst.outputKeyClass)
        throw new IllegalArgumentException("mapper output key must be equal to reducer input key")
      if (_reducer.get.inputValueClass != mapperInst.outputValueClass)
        throw new IllegalArgumentException("mapper output value must be equal to reducer input value")
    }

    this.setJarByClass(mapperInst.getClass)
    this.setMapperClass(mapperInst.getClass)
    this.setMapOutputKeyClass(mapperInst.outputKeyClass)
    this.setMapOutputValueClass(mapperInst.outputValueClass)
    if (_reducer.isEmpty) {
      this.setOutputKeyClass(mapperInst.outputKeyClass)
      this.setOutputValueClass(mapperInst.outputValueClass)
    }
    _mapper = Some(mapperInst)

    this
  }

  def jMapper[T <: Mapper[_, _, _, _]](cls: Class[T]) = {
    this.setMapperClass(cls)
    this
  }

  def numReduceTasks(num: Int) = { this.setNumReduceTasks(num); this }

  def outputFormatClass[T <: OutputFormat[_, _]](cls: Class[T]) = { this.setOutputFormatClass(cls); this }

  def outputKeyClass(cls: Class[_]) = { this.setOutputKeyClass(cls); this }

  def outputValueClass(cls: Class[_]) = { this.setOutputValueClass(cls); this }

  def partitionerClass[T <: Partitioner[_, _]](cls: Class[T]) = { this.setPartitionerClass(cls); this }

  def reducer(reducerInst: SReducer[_, _, _, _]) = {
    if (_mapper.isDefined) {
      if (_mapper.get.outputKeyClass != reducerInst.inputKeyClass)
        throw new IllegalArgumentException("combiner input key must be equal to mapper output key")
      if (_mapper.get.outputValueClass != reducerInst.inputValueClass)
        throw new IllegalArgumentException("combiner value key must be equal to mapper output value")
    }
    if (_combiner.isDefined) {
      if (_combiner.get.inputKeyClass != reducerInst.outputKeyClass)
        throw new IllegalArgumentException("combiner output key must be equal to mapper input key")
      if (_combiner.get.inputValueClass != reducerInst.outputValueClass)
        throw new IllegalArgumentException("combiner output value must be equal to mapper input value")
    }
    this.setReducerClass(reducerInst.getClass())
    this.setOutputKeyClass(reducerInst.outputKeyClass)
    this.setOutputValueClass(reducerInst.outputValueClass)
    _reducer = Some(reducerInst)
    this
  }

  def sortComparatorClass[T <: RawComparator[_]](cls: Class[T]) = { this.setSortComparatorClass(cls); this }

  def workingDirectory(path: Path) = { this.setWorkingDirectory(path); this }

}