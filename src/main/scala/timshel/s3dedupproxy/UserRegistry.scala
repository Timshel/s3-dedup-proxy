package timshel.s3dedupproxy

import cats.effect._
import java.nio.file.{FileSystems, Files, Path, StandardWatchEventKinds, WatchEvent}
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.HashSet
import scala.jdk.CollectionConverters._

case class UserRegistry(users: TrieMap[String, String], watcher: Option[(Watcher, FiberIO[Unit])] = None) {
  def get(identity: String): Option[String] = users.get(identity)
}

case class Watcher(users: TrieMap[String, String], conf: HashSet[String], var cancelled: Boolean = false) {
  import UserRegistry._

  def watch(path: Path) = IO
    .blocking {
      val absPath  = path.toAbsolutePath
      val fileName = absPath.getFileName

      log.info(s"Starting conf watcher on: $absPath")

      val watcher = FileSystems.getDefault.newWatchService()
      absPath.getParent.register(
        watcher,
        StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_DELETE,
        StandardWatchEventKinds.ENTRY_MODIFY
      )

      while (!cancelled) {
        Option(watcher.poll(3, java.util.concurrent.TimeUnit.SECONDS)) match {
          case None => ()
          case Some(wk) =>
            log.debug(s"Polling returned $wk")
            val isUsersFileEvent = wk.pollEvents().asScala.exists { evt =>
              evt.asInstanceOf[WatchEvent[Path]].context() == fileName
            }
            if (isUsersFileEvent) {
              // Small delay to let the file finish writing
              Thread.sleep(200)
              parseAndMerge(users, conf, path)
            }
            wk.reset()
        }
      }
      log.info(s"Cancelled user watcher")
      watcher.close()
    }
}

object UserRegistry {
  val log = com.typesafe.scalalogging.Logger(classOf[UserRegistry])

  def parseUsersFile(path: Path): Option[Map[String, String]] = {
    pureconfig.ConfigSource.file(path).load[Map[String, String]] match {
      case Right(config) => Some(config)
      case Left(e) =>
        if( Files.exists(path) ) {
          log.error(s"Failed to parse users file (permission will not be modified)", e)
          None
        } else {
          log.warn(s"Missing users file, permissions will be removed")
          Some(Map.empty)
        }
    }
  }

  def parseAndMerge(users: TrieMap[String, String], conf: HashSet[String], path: Path) = {
    parseUsersFile(path) match {
      case None => ()
      case Some(fu) => {
        val old = users.keys.to(HashSet)
        val cur = conf.concat(fu.keys)

        val added   = cur.diff(old)
        val removed = old.diff(cur)

        users.addAll(fu).subtractAll(removed)

        if (added.nonEmpty) log.info(s"Users added: ${added.mkString(", ")}")
        if (removed.nonEmpty) log.info(s"Users removed: ${removed.mkString(", ")}")
      }
    }
  }

  def apply(config: GlobalConfig): Resource[IO, UserRegistry] = Resource.make {
    log.debug(s"UserRegistry init with ${config.users} and ${config.usersFile}")
    config.usersFile match {
      case None => IO.pure(UserRegistry(TrieMap.empty[String, String] ++ config.users))
      case Some(path) =>
        val users = TrieMap.empty[String, String]
        for {
          fu <- IO.blocking { parseUsersFile(path) }
          watcher = Watcher(users, config.users.keys.to(HashSet))
          fiber <- watcher.watch(path).start
        } yield {
          users.addAll(config.users).addAll(fu.getOrElse(Map.empty))
          UserRegistry(users, Some((watcher, fiber)))
        }
    }
  } { ur =>
    IO {
      log.debug(s"Closing UserRegistry ")
      ur.watcher
    }.flatMap {
      case None => IO.pure(())
      case Some((w, f)) =>
        w.cancelled = true
        f.join.map(_ => ())
    }
  }
}
