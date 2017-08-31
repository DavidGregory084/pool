package pool

import scala.concurrent.duration._

import cats.syntax.flatMap._
import com.typesafe.scalalogging._
import org.scalatest._
import monix.cats._
import monix.execution.{ Scheduler, UncaughtExceptionReporter }
import monix.eval.Task

class PoolSpec extends AsyncFlatSpec with Matchers with LazyLogging {
  implicit val scheduler = Scheduler.io(
    name = "pool-test",
    reporter = UncaughtExceptionReporter(
      logger.error(s"pool-test: Uncaught exception in IO scheduler", _)))

  def createPool[A](create: Task[A], destroy: A => Task[Unit]): (Pool[A]=> Task[Assertion]) => Task[Assertion] =
    Pool.withPool[A, Assertion](
      name = "test",
      create = Task.eval(logger.debug("Creating entry")).followedBy(create),
      destroy = a => Task.eval(logger.debug(s"Destroying entry $a")).followedBy(destroy(a)),
      minIdle = 5,
      creatorDelay = 1.second,
      creatorInterval = 1.second,
      idleTimeout = 60.seconds,
      reaperDelay = 1.second,
      reaperInterval = 1.second,
      maxEntries = 10
    )(_)

  def createPool(minIdle: Int, maxEntries: Int, idleTimeout: Duration) =
    Pool.withPool[Unit, Assertion](
      name = "test",
      create = Task.eval(logger.debug("Creating entry")),
      destroy = a => Task.eval(logger.debug(s"Destroying entry $a")),
      minIdle = minIdle,
      creatorDelay = 1.second,
      creatorInterval = 1.second,
      idleTimeout = idleTimeout,
      reaperDelay = 1.second,
      reaperInterval = 1.second,
      maxEntries = maxEntries
    )(_)

  def sleep(ms: Int, fork: Boolean = true) = {
    if (fork)
      Task.fork(Task.eval(Thread.sleep(ms.toLong)))
    else
      Task.eval(Thread.sleep(ms.toLong))
  }

  "Pool" should "have minIdle resources at idle" in {
    createPool(5, 10, 60.seconds) { pool =>
      for {
        _ <- sleep(2000, fork = false)
        inUse <- pool.inUse.take
        _ <- pool.inUse.put(inUse)
      } yield inUse shouldBe pool.minIdle
    }.runAsync
  }

  it should "not have more than maxEntries resources" in {
    createPool(5, 5, 60.seconds) { pool =>
      for {
        _ <- {
          pool.withResource(_ => sleep(50))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
            .zip(pool.withResource(_ => sleep(50)))
        }
        inUse <- pool.inUse.take
        _ <- pool.inUse.put(inUse)
      } yield inUse shouldBe <=(pool.maxEntries)
    }.runAsync
  }

  it should "remove resources after idleTimeout" in {
    createPool(5, 5, 2.second) { pool =>
      for {
        _ <- sleep(5000, fork = false)
        es <- pool.entries.take
        _ <- pool.entries.put(es)
        now <- Task.eval(System.nanoTime)
        timeoutTime = now - pool.idleTimeout.toNanos
        lastUsages = es.map(_.lastUse)
      } yield all(lastUsages) should be >= timeoutTime
    }.runAsync
  }
}
