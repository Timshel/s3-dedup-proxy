import cats.effect._
import cats.effect.kernel.Deferred
import cats.effect.unsafe.IORuntime
import com.google.common.hash.HashCode;
import munit.CatsEffectSuite
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext
import skunk._

import timshel.s3dedupproxy.{Application, Database, Metadata}

class PgIntegrationTests extends CatsEffectSuite {
  import Database.*

  val database = ResourceSuiteLocalFixture(
    "application",
    Application
      .config()
      .flatMap(config =>
        Application
          .pool(config)
          .map(pool => TestDatabase(pool))
          .preAllocate {
            Application.migration(config.db)
          }
      )
  )

  override def munitFixtures = List(database)

  def use[T](f: TestDatabase => IO[T]): IO[T] = {
    IO(database()).flatMap(f)
  }

  test("Advisory lock") {
    val hashCode  = HashCode.fromLong(12);
    val hashCode2 = HashCode.fromLong(24);

    use { database =>
      for {
        _       <- assertIO(database.withAdvisoryLock(hashCode) { _ => IO.pure(true) }, true)
        _       <- assertIO(database.withAdvisoryLock(hashCode) { _ => IO.pure(true) }, true)
        block   <- Deferred[IO, Boolean]
        flag    <- Deferred[IO, Boolean]
        blocker <- database.withAdvisoryLock(hashCode) { _ => block.get }.start
        blocked <- database.withAdvisoryLock(hashCode) { _ => flag.complete(true) }.start
        _       <- assertIO(database.withAdvisoryLock(hashCode2) { _ => IO.pure(true) }, true)
        _       <- assertIO(flag.tryGet, None)
        _       <- block.complete(true)
        _       <- assertIO(blocker.join.map(_.isSuccess), true)
        _       <- assertIO(blocked.join.map(_.isSuccess), true)
      } yield ()
    }
  }

  test("Advisory shared lock") {
    val hashCode  = HashCode.fromLong(12);
    val hashCode2 = HashCode.fromLong(24);

    use { database =>
      for {
        block   <- Deferred[IO, Boolean]
        blocker <- database.withAdvisorySharedLock(hashCode) { _ => block.get }.start
        _       <- assertIO(database.withAdvisorySharedLock(hashCode) { _ => IO.pure(true) }, true)
        flag1   <- Deferred[IO, Boolean]
        flag2   <- Deferred[IO, Boolean]
        blocker <- database.withAdvisorySharedLock(hashCode) { _ => block.get }.start
        blocked <- database.withAdvisoryLock(hashCode) { _ => flag1.complete(true) }.start
        shared  <- database.withAdvisorySharedLock(hashCode) { _ => flag2.complete(true) }.start
        _       <- assertIO(flag1.tryGet, None)
        _       <- assertIO(flag2.tryGet, None)
        _       <- block.complete(true)
        _       <- assertIO(blocker.join.map(_.isSuccess), true)
        _       <- assertIO(blocked.join.map(_.isSuccess), true)
        _       <- assertIO(shared.join.map(_.isSuccess), true)
      } yield ()
    }
  }

  test("Function hash_key") {
    val hashCode  = HashCode.fromLong(12);
    val hashCode2 = HashCode.fromLong(24);
    val hashCode3 = HashCode.fromString("247d9250d3c7a567036857af9b602def8b4415f28176ec23552bd96b8875a147")
    val hashCode4 = HashCode.fromString("013095eb019fb48dd43d3b80e2ea2a008edea8bd26e64da390ab5877e911bf26")

    use { database =>
      for {
        _ <- assertIO(database.compute_key(hashCode), Database.lockKey(hashCode))
        _ <- assertIO(database.compute_key(hashCode2), Database.lockKey(hashCode2))
        _ <- assertIO(database.compute_key(hashCode3), Database.lockKey(hashCode3))
        _ <- assertIO(database.compute_key(hashCode4), Database.lockKey(hashCode4))
      } yield ()
    }
  }

  test("Query file_metadata run against db") {
    val hashCode  = HashCode.fromLong(12);
    val hashCode2 = HashCode.fromLong(24);

    use { database =>
      for {
        _ <- database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _ <- database.putMetadata(hashCode, hashCode, 12L, "B", "CT")
        _ <- assertIO(database.checkMetadata(hashCode), Some(Metadata(10L, "B", "CT")))
        _ <- database.putMetadata(hashCode2, hashCode2, 12L, "B", "CT")
        _ <- database.putMapping("A", "bucket", "B", hashCode2)
        _ <- assertIO(database.getDangling(1000), List.empty)
        _ <- assertIO(database.oldMetadatas(), 2)
        _ <- assertIO(database.getDangling(1000), List(hashCode))
        _ <- assertIO(database.delDanglingMetadatas(List(hashCode, hashCode2)), 1) // Check that hashCode2 is not deleted
        _ <- assertIO(database.delMappings("A", "bucket"), 1)
        _ <- assertIO(database.getDangling(1000), List(hashCode2))
        _ <- assertIO(database.delDanglingMetadatas(List(hashCode2)), 1)
        _ <- assertIO(database.checkMetadata(hashCode), None)
      } yield ()
    }
  }

  test("Dangling selection should lock") {
    val hashCode  = HashCode.fromLong(12);
    val hashCode2 = HashCode.fromLong(24);

    use { database =>
      for {
        _    <- database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _    <- assertIO(database.oldMetadatas(), 1)
        flag <- Deferred[IO, Boolean]
        blocked <- database.locksReleaseBlock { s =>
          for {
            _       <- assertIO(database.getDangling(1000)(s), List(hashCode))
            blocked <- database.withAdvisoryLock(hashCode) { _ => flag.complete(true) }.start
            _       <- assertIO(database.withAdvisorySharedLock(hashCode2) { _ => IO.pure(true) }, true)
            _       <- assertIO(flag.tryGet, None)
          } yield blocked
        }
        _ <- assertIO(blocked.join.map(_.isSuccess), true)
      } yield ()
    }
  }

  test("Check Metadata should evict from cleanup") {
    val hashCode = HashCode.fromLong(12);

    use { database =>
      for {
        _ <- database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _ <- assertIO(database.oldMetadatas(), 1)
        _ <- assertIO(database.getDangling(1000), List(hashCode))
        _ <- assertIO(database.getDangling(1000), List(hashCode))
        _ <- database.checkMetadata(hashCode)
        _ <- assertIO(database.getDangling(1000), List.empty)
        _ <- assertIO(database.delDanglingMetadatas(List(hashCode)), 1)
      } yield ()
    }
  }

  test("Query file_mappings run against db") {
    val hashCode = HashCode.fromLong(12);

    use { database =>
      for {
        _ <- database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- assertIO(database.getMappings("A", "bucket").map(_._1.map(_.hash)), List(hashCode))
        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- assertIO(database.getMappingHash("A", "bucket", "B"), Some(hashCode))
        _ <- assertIO(database.getMapping("A", "bucket", "B").map(_.map(_.hash)), Some(hashCode))
        _ <- assertIO(database.countMappings(hashCode), 1L)
        _ <- assertIO(database.delMapping("A", "bucket", "B"), 1)
        _ <- assertIO(database.countMappings(hashCode), 0L)

        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- database.putMapping("A", "bucket", "C", hashCode)
        _ <- assertIO(database.delMappingKeys("A", "bucket", List("B", "C")), 2)

        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- database.putMapping("B", "bucket", "B", hashCode)
        _ <- assertIO(database.countMappings("A", "bucket"), 1L)
        _ <- assertIO(database.delMappings(List(("A", "bucket", "B"), ("B", "bucket", "B"))), 2)

        _ <- database.putMapping("A", "bucket", "B", hashCode)
        _ <- database.putMapping("A", "bucket", "C", hashCode)
        _ <- assertIO(database.delMappings("A", "bucket"), 2)

        _ <- database.putMapping("A", "bucket", "prefix1", hashCode)
        _ <- database.putMapping("A", "bucket", "prefix2", hashCode)
        _ <- database.putMapping("A", "bucket", "wrefix2", hashCode)
        _ <- assertIO(database.countMappings("A", "bucket"), 3L)
        _ <- assertIO(database.delMappings("A", "bucket", "prefix"), 2)
        _ <- assertIO(database.delMappings("A", "bucket", "wrefix"), 1)

        _ <- database.putMapping("A", "bucket", "prefix1", hashCode)
        _ <- database.putMapping("A", "bucket", "prefix2", hashCode)
        _ <- database.putMapping("A", "bucket", "prefix2/toto/file1", hashCode)
        _ <- database.putMapping("A", "bucket", "prefix2/tata/file2", hashCode)
        _ <- assertIO(
          database.testDelimitedMappings("A", "bucket", Delimiter("/")),
          (
            List(
              ("prefix1", Some(hashCode), "prefix1"),
              ("prefix2", Some(hashCode), "prefix2"),
              ("prefix2/", None, "prefix2/toto/file1")
            ),
            None
          )
        )
        _ <- assertIO(
          database.testDelimitedMappings("A", "bucket", Delimiter("/"), maxResults = Some(2)),
          (
            List(
              ("prefix1", Some(hashCode), "prefix1"),
              ("prefix2", Some(hashCode), "prefix2")
            ),
            Some("prefix2")
          )
        )
        _ <- assertIO(
          database.testDelimitedMappings("A", "bucket", Delimiter("/"), marker = Some(Marker("prefix2")), maxResults = Some(2)),
          (
            List(("prefix2/", None, "prefix2/toto/file1")),
            None
          )
        )
        _ <- assertIO(
          database.testDelimitedMappings("A", "bucket", Delimiter("/"), marker = Some(Marker("prefix1")), maxResults = Some(2)),
          (
            List(
              ("prefix2", Some(hashCode), "prefix2"),
              ("prefix2/", None, "prefix2/toto/file1")
            ),
            Some("prefix2/toto/file1")
          )
        )
        _ <- assertIO(
          database.testDelimitedMappings("A", "bucket", Delimiter("/"), marker = Some(Marker("prefix2/toto/file1"))),
          (List(), None)
        )
        _ <- assertIO(
          database
            .testDelimitedMappings("A", "bucket", Delimiter("/"), Some(Prefix("prefix2/")), maxResults = Some(2)),
          (
            List(
              ("prefix2/tata/", None, "prefix2/tata/file2"),
              ("prefix2/toto/", None, "prefix2/toto/file1")
            ),
            Some("prefix2/toto/file1")
          )
        )
        _ <- assertIO(
          database
            .testDelimitedMappings("A", "bucket", Delimiter("/"), Some(Prefix("prefix2/tata/")), maxResults = Some(2)),
          (List(("prefix2/tata/file2", Some(hashCode), "prefix2/tata/file2")), None)
        )
        _ <- assertIO(database.delMappings("A", "bucket"), 4)

        _ <- assertIO(database.delDanglingMetadatas(List(hashCode)), 1)
        _ <- assertIO(database.countMappings(hashCode), 0L)
      } yield ()
    }
  }
}
