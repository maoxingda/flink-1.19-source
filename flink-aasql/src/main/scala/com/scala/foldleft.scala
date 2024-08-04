package com.scala

/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * foldLeft[B](z: B)(@_root_.scala.deprecatedName op: (B, A) => B): B =
 * B 是累积器的类型，它与最终结果的类型相同。
 * z 是累积器的初始值。
 * op 是一个函数，它接受两个参数：一个是当前的累积器值（类型为 B），
 * 另一个是集合中的当前元素（类型为 A），并返回一个新的累积器值（类型为 B）。
 */
object foldleft {
  def main(args: Array[String]): Unit = {
    val numbers = List(1, 2, 3, 4, 5)
    // 使用 foldLeft 计算总和
    val sum = numbers.foldLeft(100)(_ + _)
    println(sum) // 输出: 15
  }
}
