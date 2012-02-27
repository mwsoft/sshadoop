package jp.mwsoft.sshadoop

import org.apache.hadoop.mapreduce.{ Mapper }

abstract class SMapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT](
  implicit keyInType: Manifest[KEY_IN], valInType: Manifest[VAL_IN], keyOutType: Manifest[KEY_OUT], valOutType: Manifest[VAL_OUT] )
    extends Mapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT] with ImplicitConversions {

  def inputKeyClass = keyInType.erasure.asInstanceOf[Class[KEY_IN]]
  def inputValueClass = valInType.erasure.asInstanceOf[Class[VAL_IN]]
  def outputKeyClass = keyOutType.erasure.asInstanceOf[Class[KEY_OUT]]
  def outputValueClass = valOutType.erasure.asInstanceOf[Class[VAL_OUT]]

  type Context = Mapper[KEY_IN, VAL_IN, KEY_OUT, VAL_OUT]#Context
}
