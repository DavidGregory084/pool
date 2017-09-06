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
        Task.eval(logger.trace(s"$name: Using resource ${e.entry}")).followedBy {
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
              Task.eval(logger.debug(s"$name: Creating a new resource")).followedBy {
                create.materialize.flatMap {
                  case Success(a) =>
                    entries.put(Nil).followedBy(Task.now(a))
                  case Failure(NonFatal(e)) =>
                    // We failed to create the resource, so decrement the counter again
                    inUse.take.flatMap(u => inUse.put(u - 1)).followedBy {
                      entries.put(Nil).followedBy(Task.raiseError(e))
                    }
                  case Failure(e) =>
                    Task.raiseError(e)
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
        Task.eval(logger.trace(s"$name: Using resource ${e.entry}")).followedBy {
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
              Task.eval(logger.debug(s"$name: Creating a new resource")).followedBy {
                create.materialize.flatMap {
                  case Success(a) =>
                    entries.put(Nil).followedBy(Task.now(Some(a)))
                  case Failure(NonFatal(e)) =>
                    // We failed to create the resource, so decrement the counter again
                    inUse.take.flatMap(u => inUse.put(u - 1)).followedBy {
                      entries.put(Nil).followedBy(Task.raiseError(e))
                    }
                  case Failure(e) =>
                    Task.raiseError(e)
                }
              }
            }
          }
        }
    }
  }

  private def putResource(a: A): Task[Unit] = {
    Task.eval(logger.trace(s"$name: Returning $a to pool")).followedBy {
      entries.take.flatMap { es =>
        entries.put(Entry(a, System.nanoTime) :: es)
      }
    }
  }

  private def destroyResource(a: A): Task[Unit] = {
    for {
      _ <- Task.eval(s"$name: Destroying resource $a")
      // Ignore non-fatal errors in resource destruction
      _ <- destroy(a).recover { case NonFatal(_) => }
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
    _ <- es.traverse { e =>
      destroy(e.entry)
        .recover { case NonFatal(_) => }
    }
  } yield ()
}

object Pool extends LazyLogging {
  private val random = new scala.util.Random

  def getRetryDelay(consecutiveFailed: Int): FiniteDuration = {
    // TODO: Add retry delay cap to config
    // Cap delay at 60 seconds between attempts
    val cap = 60.seconds.toMillis.toInt
    val exp = math.pow(2, consecutiveFailed.toDouble)
    // Choose the min of the cap and the calculated delay
    val max = math.min(cap, 250 * 2 * exp.toInt)
    // The next delay is a random number of milliseconds
    // between 0 and the calculated max delay
    val nextDelay = Pool.random.nextInt(max + 1)
    nextDelay.milliseconds
  }

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

          def addEntries(delay: Option[FiniteDuration], consecutiveFailed: Int = 0): Task[Unit] =
            for {
              // Sleep for the specified delay before attempting to create a resource
              _ <- delay.map { d =>
                Task.eval(Thread.sleep(d.toMillis))
              }.getOrElse(Task.unit)

              // Take the MVars describing the pool status
              es <- entries.take
              u <- inUse.take

              esString = es.mkString("[", ",", "]")

              _ <- Task.eval(logger.trace(s"$poolName: Adding idle entries, entries: $esString, inUse: $u"))

              // If there are fewer than the specified number of idle resources, add another
              _ <- if (u < minIdle) {
                // Increment the number of used resources
                inUse.put(u + 1).followedBy {
                  create.materialize.flatMap {
                    // We successfully created a new entry;
                    // Update the entries MVar and do a recursive
                    // call to see whether we need to create another
                    case Success(a) =>
                      val entry = Entry(a, System.nanoTime)
                      entries.put(entry :: es)
                        .followedBy(addEntries(delay = None, consecutiveFailed = 0))

                    // We failed to create a new entry;
                    // Retry using a capped exponential backoff strategy with jitter
                    case Failure(NonFatal(e)) =>
                      val nextFailed = consecutiveFailed + 1
                      val nextDelay = getRetryDelay(nextFailed)
                      // We failed to create the resource, so decrement the counter again
                      Task.eval(logger.error(s"Failed to create resource - retrying after $nextDelay", e)).followedBy {
                        inUse.take.flatMap(uu => inUse.put(uu - 1))
                          .followedBy(entries.put(es))
                          .followedBy(addEntries(delay = Some(nextDelay), consecutiveFailed = nextFailed))
                      }

                    // Virtual machine error - don't even try to handle that
                    case Failure(e) =>
                      Task.raiseError(e)

                  }.recover { case NonFatal(_) => }
                }
              } else {
                // Otherwise just put back the values we took
                inUse.put(u).followedBy(entries.put(es))
              }
            } yield ()

          Await.result(addEntries(delay = None).runAsync(scheduler), Duration.Inf)
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
            _ <- stale.traverse { e =>
              destroy(e.entry)
                .recover { case NonFatal(_) => }
            }

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
    Task.eval(logger.info(s"Creating pool $name - idleTimeout=$idleTimeout, minIdle=$minIdle, maxEntries=$maxEntries")).followedBy {
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
                logger.error(s"$name: Uncaught exception in creator thread", _)
              )
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
                logger.error(s"$name: Uncaught exception in reaper thread", _)
              )
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
}
