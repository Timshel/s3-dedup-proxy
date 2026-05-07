import cats.effect._
import cats.effect.unsafe.IORuntime
import com.google.common.hash.HashCode;
import skunk._
import skunk.data.Completion
import skunk.implicits._
import skunk.codec.all._

import timshel.s3dedupproxy.{Database, Metadata}

class TestDatabase(
    pool: Resource[IO, Session[IO]]
) extends Database(pool)(using IORuntime.global) {
  import Database.*

  def apply(pool: Resource[IO, Session[IO]]): TestDatabase = new TestDatabase(pool)

  val sharedLockQ: Query[Long, Void] = sql"SELECT pg_advisory_lock_shared($int8)".query(void)

  val sharedUnlockQ: Query[Long, Boolean] = sql"SELECT  pg_advisory_unlock_shared($int8)".query(bool)

  /** Acquires a session scoped shared advisory lock keyed on the hash.
    * Uses the first 8 bytes of the hash as the lock key for good distribution.
    */
  def withAdvisorySharedLock[A](hash: HashCode)(body: Session[IO] => IO[A]): IO[A] = {
    val lockKey = Database.lockKey(hash)
    pool.use { s =>
      s.prepare(sharedLockQ)
        .flatMap(_.unique(lockKey))
        .flatMap { _ => body(s) }
        .guarantee {
          s.prepare(sharedUnlockQ).flatMap(_.unique(lockKey)).map(_ => ())
        }
    }
  }

  def compute_key(hash: HashCode): IO[Long] = {
    val query: Query[HashCode, Long] = sql"""SELECT hash_key(${Database.hashE})""".query(int8)

    pool.use {
      _.prepare(query).flatMap(_.unique(hash))
    }
  }

  // Helper to bypass the Dangling 1h protection
  def oldMetadatas(): IO[Int] = {
    val cmd = sql"""
      UPDATE file_metadata SET checked = checked - INTERVAL '2 hour'
    """.command

    pool.use {
      _.prepare(cmd)
        .flatMap { pc => pc.execute(skunk.Void) }
        .map {
          case Completion.Update(count) => count
          case _                        => throw new AssertionError("oldMetadatas execution should only return Update")
        }
    }
  }

  def checkMetadata(hashCode: HashCode): IO[Option[Metadata]] = pool.use(super.checkMetadata(hashCode)(_))

  def putMetadata(hash: HashCode, md5: HashCode, size: Long, eTag: String, contentType: String): IO[Completion] =
    pool.use(super.putMetadata(hash, md5, size, eTag, contentType)(_))

  override def getDangling(limit: Int)(session: Session[IO]): IO[List[HashCode]] = super.getDangling(limit)(session)

  def getDangling(limit: Int): IO[List[HashCode]] = locksReleaseBlock(super.getDangling(limit)(_))

  def delDanglingMetadatas(hashes: List[HashCode]): IO[Int] = pool.use(super.delDanglingMetadatas(hashes)(_))

  def testDelimitedMappings(
      user_name: String,
      bucket: String,
      delimiter: Delimiter,
      prefix: Option[Prefix] = None,
      marker: Option[Marker] = None,
      maxResults: Option[Int] = None
  ): IO[(List[(String, Option[HashCode], String)], Option[String])] = {
    super.getDelimitedMappings(user_name, bucket, delimiter, prefix, marker, maxResults).map { case (values, last) =>
      values.map { case (deli, mapping, key) => (deli, mapping.map(_.hash), key) } -> last
    }
  }
}
