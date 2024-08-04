package com.scala

object demo {
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 函数里面调用函数=高阶函数
   */
  def main(args: Array[String]): Unit = {
    meth1(metho2)
  }
  def meth1(f :  => String) : Unit = {
    println(f)
  }
  def metho2() : String = {
    "测试高阶函数"
  }
  def t (i : Int) : Unit ={

  }

}
