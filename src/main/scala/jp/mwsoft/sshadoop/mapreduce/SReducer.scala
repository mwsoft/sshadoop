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

import org.apache.hadoop.mapreduce.{ Reducer }

abstract class SReducer[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT](
  implicit keyInType: Manifest[KEY_IN], valInType: Manifest[VAL_IN], keyOutType: Manifest[KEY_OUT], valOutType: Manifest[VAL_OUT])
    extends Reducer[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT] with ImplicitConversions {

  type Context = Reducer[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT]#Context

  def reduce(key: KEY_IN, values: Iterator[VAL_IN], context: Context)

  override def reduce(key: KEY_IN, values: java.lang.Iterable[VAL_IN], context: Context) {
    reduce(key, javaIterator2scalaIterator(values.iterator()), context)
  }

  def inputKeyClass = keyInType.erasure.asInstanceOf[Class[KEY_IN]]
  def inputValueClass = valInType.erasure.asInstanceOf[Class[VAL_IN]]
  def outputKeyClass = keyOutType.erasure.asInstanceOf[Class[KEY_OUT]]
  def outputValueClass = valOutType.erasure.asInstanceOf[Class[VAL_OUT]]
}