/*
 * Copyright 2017 David Gregory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pool

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{ Success, Failure }

import cats.instances.list._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import com.typesafe.scalalogging._
import monix.cats._
import monix.eval.{ MVar, Task }
import monix.execution.{ Cancelable, CancelableFuture, Scheduler, UncaughtExceptionReporter }
import monix.execution.misc.NonFatal

final case class Entry[A](entry: A, lastUse: Long)

final case class Pool[A](
    name: String,
    entries: MVar[List[Entry[A]]],
    inUse: MVar[Int],
    create: Task[A],
    destroy: A => Task[Unit],
    idleTimeout: Duration,
    minIdle: Int,
    maxEntries: Int,
    private val finalizer: Task[Unit]
) extends LazyLogging {
  private def takeResource: Task[A] = {
    // Take the entries list
    entries.take.flatMap {
      case e :: es =>
        // If there is a free resource take it
        Task.eval(logger.info(s"$name: Using resource ${e.entry}")).followedBy {
          entries.put(es).followedBy { Task.now(e.entry) }
        }
      case Nil =>
        inUse.take.flatMap { u =>
          // Otherwise, if we have reached the maximum number of resources, retry
          if (u == maxEntries) {
            Task.eval(logger.trace(s"$name: Maximum resource count reached, waiting...")).followedBy {
              inUse.put(u).followedBy(entries.put(Nil)).followedBy(takeResource)
            }
          } else {
            // If we haven't, create a new resource and return it
            inUse.put(u + 1).followedBy {
              Task.eval(logger.info(s"$name: Creating a new resource")).followedBy {
                create.materialize.flatMap {
                  case Success(a) =>
                    entries.put(Nil).followedBy(Task.now(a))
                  case Failure(e) =>
                    // We failed to create the resource, so decrement the counter again
                    inUse.take.flatMap(u => inUse.put(u - 1)).followedBy {
                      entries.put(Nil).followedBy(Task.raiseError(e))
                    }
                }
              }
            }
          }
        }
    }
  }

  private def tryTakeResource: Task[Option[A]] = {
    entries.take.flatMap {
      case e :: es =>
        // If there is a free resource take it
        Task.eval(logger.info(s"$name: Using resource ${e.entry}")).followedBy {
          entries.put(es).followedBy { Task.now(Some(e.entry)) }
        }
      case Nil =>
        inUse.take.flatMap { u =>
          // Otherwise, if we have reached the maximum number of resources, return None
          if (u == maxEntries) {
            Task.eval(logger.trace(s"$name: Maximum resource count reached")).followedBy {
              inUse.put(u).followedBy(entries.put(Nil)).followedBy(Task.now(None))
            }
          } else {
            // If we haven't, create a new resource and return it
            inUse.put(u + 1).followedBy {
              Task.eval(logger.info(s"$name: Creating a new resource")).followedBy {
                create.materialize.flatMap {
                  case Success(a) =>
                    entries.put(Nil).followedBy(Task.now(Some(a)))
                  case Failure(e) =>
                    // We failed to create the resource, so decrement the counter again
                    inUse.take.flatMap(u => inUse.put(u - 1)).followedBy {
                      entries.put(Nil).followedBy(Task.raiseError(e))
                    }
                }
              }
            }
          }

        }
    }
  }

  private def putResource(a: A): Task[Unit] = {
    Task.eval(logger.info(s"$name: Returning $a to pool")).followedBy {
      entries.take.flatMap { es =>
        entries.put(Entry(a, System.nanoTime) :: es)
      }
    }
  }

  private def destroyResource(a: A): Task[Unit] = {
    for {
      _ <- Task.eval(s"$name: Destroying resource $a")
      // Ignore non-fatal errors in resource destruction
      _ <- destroy(a).recoverWith { case NonFatal(_) => Task.unit }
      u <- inUse.take
      _ <- inUse.put(u - 1)
    } yield ()
  }

  def withResource[B](f: A => Task[B]): Task[B] =
    takeResource.flatMap { a =>
      f(a).materialize.flatMap {
        case Success(b) =>
          putResource(a).followedBy { Task.now(b) }
        case Failure(NonFatal(e)) =>
          destroyResource(a).followedBy { Task.raiseError(e) }
        case Failure(e) =>
          Task.raiseError(e)
      }
    }

  def tryWithResource[B](f: A => Task[B]): Task[Option[B]] =
    tryTakeResource.flatMap { optA =>
      optA.fold(Task.now(Option.empty[B])) { a =>
        f(a).materialize.flatMap {
          case Success(b) =>
            putResource(a).followedBy { Task.now(Some(b)) }
          case Failure(NonFatal(e)) =>
            destroyResource(a).followedBy { Task.raiseError(e) }
          case Failure(e) =>
            Task.raiseError(e)
        }
      }
    }

  private[pool] def close: Task[Unit] = for {
    _ <- Task.eval(logger.info(s"$name: Closing pool"))
    // Shut down the creator and reaper threads
    _ <- finalizer
    // Take the MVars
    es <- entries.take
    u <- inUse.take
    // Put back empty values
    _ <- inUse.put(0)
    _ <- entries.put(Nil)
    // Destroy every pool entry
    _ <- es.traverse(e => destroy(e.entry))
  } yield ()
}

object Pool extends LazyLogging {
  private def creator[A](
    poolName: String,
    create: Task[A],
    minIdle: Int,
    awakeDelay: FiniteDuration,
    awakeInterval: FiniteDuration,
    entries: MVar[List[Entry[A]]],
    inUse: MVar[Int]
  ): Task[Unit] =
    Task.fork {
      Task.async { (scheduler, _) =>
        val runnable: Runnable = { () =>
          val addEntries = for {
            // Take the MVars describing the pool status
            es <- entries.take
            u <- inUse.take

            esString = es.mkString("[", ",", "]")

            _ <- Task.eval(logger.trace(s"$poolName: Adding idle entries, entries: $esString, inUse: $u"))

            _ <- if (u < minIdle) {
              // If there are fewer than the specified number of idle resources, add some more
              val newResources = (0 until (minIdle - u)).toList.traverse(_ => create)
              newResources.materialize.flatMap {
                case Success(newAs) =>
                  val newEs = newAs.map(Entry(_, System.nanoTime))
                  inUse.put(u + newEs.length).followedBy(entries.put(es ++ newEs))
                case Failure(NonFatal(_)) =>
                  inUse.put(u).followedBy(entries.put(es))
                case Failure(e) =>
                  Task.raiseError(e)
              }
            } else {
              // Otherwise just put back the values we took
              inUse.put(u).followedBy(entries.put(es))
            }
          } yield ()

          Await.result(addEntries.runAsync(scheduler), Duration.Inf)
        }

        scheduler.scheduleWithFixedDelay(awakeDelay.toMillis, awakeInterval.toMillis, TimeUnit.MILLISECONDS, runnable)
      }
    }

  private def reaper[A](
    poolName: String,
    destroy: A => Task[Unit],
    idleTimeout: FiniteDuration,
    awakeDelay: FiniteDuration,
    awakeInterval: FiniteDuration,
    entries: MVar[List[Entry[A]]],
    inUse: MVar[Int]
  ): Task[Unit] =
    Task.fork {
      // Create a task that runs the cleanup task and never completes
      Task.async { (scheduler, _) =>
        val runnable: Runnable = { () =>
          val removeStale = for {
            // Take the MVars describing the pool status
            es <- entries.take
            u <- inUse.take

            esString = es.mkString("[", ",", "]")

            _ <- Task.eval(logger.trace(s"$poolName: Removing stale entries, entries: $esString, inUse: $u"))

            time = System.nanoTime

            // Split the list of current entries into stale (timed out) and live entries
            (stale, live) = es.partition { e =>
              val last = e.lastUse
              val diff = time - last
              val timeout = idleTimeout.toNanos
              diff > timeout
            }

            _ <- if (stale.nonEmpty) {
              // If there are any stale entries alter the MVars accordingly
              inUse.put(u - stale.length).followedBy(entries.put(live))
            } else {
              // Otherwise just put back the values we took
              inUse.put(u).followedBy(entries.put(es))
            }

            // Run the cleanup action for each stale entry
            _ <- stale.traverse(e => destroy(e.entry))

          } yield ()

          Await.result(removeStale.runAsync(scheduler), Duration.Inf)
        }

        scheduler.scheduleWithFixedDelay(awakeDelay.toMillis, awakeInterval.toMillis, TimeUnit.MILLISECONDS, runnable)
      }
    }

  private def create[A](
    name: String,
    create: Task[A],
    destroy: A => Task[Unit],
    idleTimeout: Duration,
    creatorDelay: FiniteDuration,
    creatorInterval: FiniteDuration,
    reaperDelay: FiniteDuration,
    reaperInterval: FiniteDuration,
    minIdle: Int,
    maxEntries: Int
  ): Task[Pool[A]] =
    Task.eval(logger.info(s"Creating pool $name with idleTimeout=$idleTimeout, minIdle=$minIdle, maxEntries=$maxEntries")).followedBy {
      Task.create { (scheduler, callback) =>
        val inUse = MVar(0)
        val entries = MVar(List.empty[Entry[A]])

        val creatorThread =
          if (minIdle == 0)
            CancelableFuture.never
          else {
            // Start the creator thread
            val creatorTask = creator(name, create, minIdle, creatorDelay, creatorInterval, entries, inUse)
            creatorTask.runAsync(Scheduler.singleThread(
              name = s"$name-creator",
              reporter = UncaughtExceptionReporter(
                logger.error(s"$name: Uncaught exception in creator thread", _))
            ))
          }

        val reaperThread =
          if (!idleTimeout.isFinite)
            CancelableFuture.never
          else {
            // Start the reaper thread
            val reaperTask = reaper(name, destroy, idleTimeout.asInstanceOf[FiniteDuration], reaperDelay, reaperInterval, entries, inUse)
            reaperTask.runAsync(Scheduler.singleThread(
              name = s"$name-reaper",
              reporter = UncaughtExceptionReporter(
                logger.error(s"$name: Uncaught exception in reaper thread", _))
            ))
          }

        // Create a task that cancels the creator thread and the reaper thread
        val finalizer = Task.eval(creatorThread.cancel).followedBy(Task.eval(reaperThread.cancel))

        // Complete the callback by creating a pool
        val pool = Pool(name, entries, inUse, create, destroy, idleTimeout, minIdle, maxEntries, finalizer)
        callback.onSuccess(pool)

        // Canceling pool creation is the same as closing the pool
        Cancelable(() => Await.result(pool.close.runAsync(scheduler), Duration.Inf))
      }
    }

  def withPool[A, B](
    name: String,
    create: Task[A],
    destroy: A => Task[Unit],
    idleTimeout: Duration,
    creatorDelay: FiniteDuration,
    creatorInterval: FiniteDuration,
    reaperDelay: FiniteDuration,
    reaperInterval: FiniteDuration,
    minIdle: Int,
    maxEntries: Int
  )(f: Pool[A] => Task[B]): Task[B] =
    Pool.create[A](
      name,
      create, destroy,
      idleTimeout,
      creatorDelay, creatorInterval,
      reaperDelay, reaperInterval,
      minIdle, maxEntries
    ).flatMap { pool =>
      f(pool).materialize.flatMap {
        case Success(b) =>
          pool.close.followedBy(Task.now(b))
        case Failure(NonFatal(e)) =>
          pool.close.followedBy(Task.raiseError(e))
        case Failure(e) =>
          Task.raiseError(e)
      }
    }

  def main(args: Array[String]): Unit = {
    implicit val scheduler = Scheduler.io(
      name = "pool-test",
      reporter = UncaughtExceptionReporter(
        logger.error(s"pool-test: Uncaught exception in IO scheduler", _)))

    val createPool = Pool.withPool[Unit, Unit](
      name = "test",
      create = Task.eval(logger.info("Creating entry")),
      destroy = a => Task.eval(logger.info(s"Destroying entry $a")),
      minIdle = 5,
      creatorDelay = 1.second,
      creatorInterval = 1.second,
      idleTimeout = 60.seconds,
      reaperDelay = 1.second,
      reaperInterval = 1.second,
      maxEntries = 10
    ) { pool =>
        for {
          _ <- {
            pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
          }
          _ <- {
            pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
          }
          _ <- {
            pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
          }
          _ <- {
            pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
              .zip(pool.withResource(a => Task.fork(Task.eval(Thread.sleep(20000)))))
          }
        } yield ()
      }

    Await.result(createPool.runAsync, Duration.Inf)
  }
}
