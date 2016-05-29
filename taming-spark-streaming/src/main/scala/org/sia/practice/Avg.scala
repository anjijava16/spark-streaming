package org.sia.practice

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author dbiswas
 * Simple Average Calculation
 */
object Avg {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Average").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(1, 2, 3, 4, 5))

    val output = input.aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val avg = output._1 / output._2.toDouble

    //Print the average calculated
    println(avg)

  }
}