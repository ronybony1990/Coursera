package HighThroughputProducerConsumer

import java.sql.Timestamp
import java.util.{Calendar, TimeZone}
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.ExecutionContext
import scala.util.Success

/*class Consumer(consumerMonitor: ConsumerMonitor,
               buffer: SharedBuffer[Int],
               sleepTime: Int /* for testing*/,
               ec: ExecutionContext) {

  private val worker: Worker = new Worker()
  private val isStopped = new AtomicBoolean(false)
  private val thread: AtomicReference[Thread] = new AtomicReference[Thread](Thread.currentThread())

  def consume(): Unit = {
    while(!isStopped.get) {
      try {
        val itemFromQ: Option[Int] = buffer.take()
        itemFromQ match {
          case Some(item) =>
            val startTime = new Timestamp(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime.getTime)
            val fut = worker.doSomeWork(item, sleepTime)(ec)
            fut.onComplete{
              case Success(startTime) =>
                val finishTime = new Timestamp(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime.getTime)
                // do what ever
                consumerMonitor.reportMetrics(
                  timeTakenToFinish=finishTime - startTime,
                  queueSize = buffer.getItemsInBuffer
                )
            }(ec)
          case None =>
          // got nothing
        }
      }
      catch {
        case e: InterruptedException =>
          //cancel futures
          println("Closing down consumer")
      }
    }
  }

  def stopConsumer(): Unit = {
    if (isStopped.compareAndSet(false, true)) thread.get().interrupt()
    else println("Consumer is already stopped") // throw exception depending on use case
  }

}
*/