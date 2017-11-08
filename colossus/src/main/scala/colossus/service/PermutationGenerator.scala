package colossus.service

import scala.collection.mutable

/**
  * The PermutationGenerator creates permutations such that consecutive calls
  * are guaranteed to cycle though all items as the first element.
  *
  * This currently doesn't iterate through every possible permutation, but it
  * does evenly distribute 1st and 2nd tries...needs some more work
  */
class PermutationGenerator[T](seedlist: Seq[T]) extends Iterator[List[T]] {
  private val items = mutable.ListBuffer.empty[T]
  items ++= seedlist

  private var swapIndex = 1

  private val cycleSize  = seedlist.size * (seedlist.size - 1)
  private var cycleCount = 0

  def hasNext = true

  private def swap(indexA: Int, indexB: Int) {
    val tmp = items(indexA)
    items(indexA) = items(indexB)
    items(indexB) = tmp
  }

  def next(): List[T] = {
    if (items.length == 1) {
      items.head
    } else {
      swap(0, swapIndex)
      swapIndex += 1
      if (swapIndex == items.length) {
        swapIndex = 1
      }
      cycleCount += 1
      if (items.length > 3) {
        if (cycleCount == cycleSize) {
          cycleCount = 0
          swapIndex += 1
          if (swapIndex == items.length) {
            swapIndex = 1
          }
        }
      }

    }
    items.toList
  }
}
