## pool

**pool** is a Scala port of Bryan O'Sullivan's excellent Haskell resource-pooling library [bos/pool](https://github.com/bos/pool).

**pool** makes extensive use of the facilities provided by the [Monix](https://monix.io/) library.

### Example

```scala
implicit val scheduler = Scheduler.io(
  name = "pool-test",
  reporter = UncaughtExceptionReporter(
    logger.error(s"pool-test: Uncaught exception in IO scheduler", _)))

// Run a `Pool[Unit] => Task[Unit]`
val usePool = Pool.withPool[Unit, Unit](
  name = "test",
  create = Task.eval(logger.info("Creating entry")),
  destroy = a => Task.eval(logger.info(s"Destroying entry $a")),
  minIdle = 5,
  maxEntries = 10,
  idleTimeout = 60.seconds,
  creatorDelay = 1.second,
  creatorInterval = 1.second,
  reaperDelay = 1.second,
  reaperInterval = 1.second
) { pool =>

    pool.withResource(a => Task.fork(Task.eval(Thread.sleep(5000))))
  }

Await.result(usePool.runAsync, Duration.Inf)
```
