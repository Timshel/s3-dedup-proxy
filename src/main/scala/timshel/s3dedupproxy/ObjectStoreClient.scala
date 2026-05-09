package timshel.s3dedupproxy

import cats.effect._
import io.minio.{MinioClient, RemoveObjectsArgs};
import io.minio.messages.DeleteObject;
import com.google.common.hash.HashCode;

final case class ObjectStoreClient(
    config: BackendConfig
) {
  import ObjectStoreClient._

  val client = MinioClient
    .builder()
    .endpoint(config.endpoint)
    .credentials(config.accessKeyId, config.secretAccessKey)
    .build()

  def close(): Unit = client.close()

  def deleteKeys(hashes: List[HashCode]): IO[(List[HashCode], List[HashCode])] = IO.blocking {
    import scala.jdk.CollectionConverters.IterableHasAsScala

    if (hashes.nonEmpty) {
      val objects = new java.util.LinkedList[DeleteObject]();

      val keyToHash = hashes
        .map { h =>
          val key = ProxyBlobStore.hashToKey(h)
          objects.add(new DeleteObject(key))
          key -> h
        }
        .to(scala.collection.mutable.Map)

      val failed = client
        .removeObjects(RemoveObjectsArgs.builder().bucket(config.bucket).objects(objects).build())
        .asScala
        .flatMap { r =>
          val e = r.get()
          log.error(s"Failed to delete ${e.objectName}, err ${e.code}: ${e.message}")
          keyToHash.remove(e.objectName)
        }
        .toList

      keyToHash.values.toList -> failed
    } else (List.empty, List.empty)
  }
}

object ObjectStoreClient {
  val log = com.typesafe.scalalogging.Logger(classOf[ObjectStoreClient])
}
