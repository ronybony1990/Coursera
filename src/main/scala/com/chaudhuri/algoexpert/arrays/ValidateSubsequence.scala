package com.chaudhuri.algoexpert.arrays

object Program  {
  def main(args: Array[String]): Unit = {
    assert(validateSubSequence(
      List(5,1, 22, 25, 6, -1, 8, 10),
      List(1,6,-1,10)
    ), "failure")
    println("success")
  }

  def validateSubSequence(arr: List[Int],
                          sequence: List[Int]): Boolean = {
    val id = (0 to sequence.length -1).foldLeft(0) {
      case (x, i) =>
        if (x >= 0) arr.indexOf(sequence(i), x + x.sign)
        else -1
    }
    id != -1
  }
}
