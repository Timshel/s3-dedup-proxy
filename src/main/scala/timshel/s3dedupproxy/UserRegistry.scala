package timshel.s3dedupproxy

import cats.effect._
import java.nio.file.{FileSystems, Files, Path, StandardWatchEventKinds, WatchEvent}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

/** Thread-safe, hot-reloadable user registry backed by a HOCON file.
  *
  * The file format is simple key-value pairs:
  * {{{
  * {
  *   tenant1: "secret-key-hex"
  *   tenant2: "secret-key-hex"
  * }
  * }}}
  *
  * The registry watches the file for changes and reloads automatically.
  * Reads are lock-free via AtomicReference (safe for Jetty's S3Proxy threads).
  */
object UserRegistry {
  val log = com.typesafe.scalalogging.Logger(classOf[Application])

  def parseUsersFile(path: Path): Map[String, String] = {
    val config = com.typesafe.config.ConfigFactory.parseFile(path.toFile)
    config
      .entrySet()
      .asScala
      .map(e => e.getKey -> e.getValue.unwrapped().toString)
      .toMap
  }

  /** Creates a UserRegistry Resource that watches the file and reloads on changes.
    * The Resource ensures the watcher thread is stopped on shutdown.
    */
  def fromFile(path: Path): Resource[IO, UserRegistry] = {
    Resource.make {
      IO.blocking {
        val absPath  = path.toAbsolutePath
        val users    = parseUsersFile(absPath)
        val registry = new UserRegistry(new AtomicReference(users))

        log.info(s"Loaded ${users.size} users from $absPath: ${users.keys.mkString(", ")}")

        // Start file watcher in a daemon thread
        val watchDir = absPath.getParent
        val fileName = absPath.getFileName
        val watcher  = FileSystems.getDefault.newWatchService()
        watchDir.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY)

        val watchThread = new Thread(() => {
          try {
            while (!Thread.currentThread().isInterrupted) {
              val key = watcher.take()
              val events = key.pollEvents().asScala
              val relevant = events.exists { evt =>
                evt.asInstanceOf[WatchEvent[Path]].context() == fileName
              }
              if (relevant) {
                // Small delay to let the file finish writing
                Thread.sleep(200)
                try {
                  val newUsers = parseUsersFile(absPath)
                  val old      = registry.users.getAndSet(newUsers)
                  val added    = newUsers.keySet -- old.keySet
                  val removed  = old.keySet -- newUsers.keySet
                  if (added.nonEmpty) log.info(s"Users added: ${added.mkString(", ")}")
                  if (removed.nonEmpty) log.info(s"Users removed: ${removed.mkString(", ")}")
                  if (added.isEmpty && removed.isEmpty) log.debug("Users file reloaded, no changes")
                } catch {
                  case e: Exception => log.error(s"Failed to reload users file: ${e.getMessage}")
                }
              }
              key.reset()
            }
          } catch {
            case _: InterruptedException => // shutdown
          }
        }, "user-registry-watcher")
        watchThread.setDaemon(true)
        watchThread.start()

        (registry, watchThread, watcher)
      }
    } { case (_, thread, watcher) =>
      IO.blocking {
        log.info("Stopping user registry file watcher")
        thread.interrupt()
        watcher.close()
      }
    }.map(_._1)
  }
}

class UserRegistry(val users: AtomicReference[Map[String, String]]) {

  /** Look up a user's secret. Thread-safe, lock-free. */
  def get(identity: String): Option[String] =
    users.get().get(identity)

  /** Get all current users. */
  def getAll: Map[String, String] =
    users.get()

  /** Check if an identity is a valid user. */
  def contains(identity: String): Boolean =
    users.get().contains(identity)
}
