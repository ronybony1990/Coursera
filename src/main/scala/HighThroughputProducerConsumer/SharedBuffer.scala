package HighThroughputProducerConsumer

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

class SharedBuffer[T](capacity: Int) {

  private val buffer = new LinkedBlockingQueue[T](capacity)
  private val lock: ReentrantLock = new ReentrantLock()
  @volatile var isComplete: Boolean = false
  private val holdUpProducer = lock.newCondition()

  private val itemsInQueue: AtomicLong = new AtomicLong(0)

  def offerWithTimeout(item: T,
                       timeoutInMillis: Int = 100): Boolean = {
    lock.lock()
    try {
      val isPublished: Boolean = buffer.offer(item, timeoutInMillis, TimeUnit.MILLISECONDS)
      if (isPublished) itemsInQueue.incrementAndGet()
      isPublished
    }
  }

  def take(): Option[T] = {
    val item = Option(buffer.poll())
    item match {
      case Some(_) =>
        itemsInQueue.decrementAndGet()
        item
      case None => item
    }
  }

  def getItemsInBuffer: Long = {
    itemsInQueue.get()
  }

}
