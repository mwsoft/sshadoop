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

import org.apache.hadoop.mapreduce.{ Mapper }

class SMapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT](
  implicit keyOutType: Manifest[KEY_OUT], valOutType: Manifest[VAL_OUT])
    extends SMapperBase[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT](keyOutType, valOutType) with ImplicitConversions

class SMapperBase[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT](
  keyOutType: Manifest[KEY_OUT], valOutType: Manifest[VAL_OUT])
    extends Mapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT] with ImplicitConversions {

  type Context = Mapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT]#Context

  def outputKeyClass = if (keyOutType != null) keyOutType.erasure.asInstanceOf[Class[KEY_OUT]] else null
  def outputValueClass = if (valOutType != null) valOutType.erasure.asInstanceOf[Class[VAL_OUT]] else null
}
