package HighThroughputProducerConsumer

import scala.concurrent.{ExecutionContext, Future}

class Worker {

  def revoverable(throwable: Throwable): Boolean = throwable match {
    case exe@(_: IllegalStateException) => true

  }
  def doSomeWork(item: Int,
                 sleepInMillis: Int)
                (implicit ec: ExecutionContext): Future[Long] = {
    Future {
      Thread.sleep(sleepInMillis)
      1L
    }
  }
}
