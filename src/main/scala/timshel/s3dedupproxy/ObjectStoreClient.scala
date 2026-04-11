package timshel.s3dedupproxy

import cats.effect._
import io.minio.{MinioClient, RemoveObjectsArgs};
import io.minio.messages.DeleteObject;
import com.google.common.hash.HashCode;

case class ObjectStoreClient(
    config: BackendConfig
) {
  import ObjectStoreClient._

  val client = MinioClient
    .builder()
    .endpoint(config.endpoint)
    .credentials(config.accessKeyId, config.secretAccessKey)
    .build()

  /** Deletes the given hashes from the backend bucket.
    * Returns the set of object keys that failed to delete.
    */
  def deleteKeys(hashes: List[HashCode]): IO[Set[String]] = IO.blocking {
    if (hashes.nonEmpty) {
      val objects = new java.util.LinkedList[DeleteObject]();

      hashes.foreach { h =>
        objects.add(new DeleteObject(ProxyBlobStore.hashToKey(h)))
      }

      val failedKeys = scala.collection.mutable.Set[String]()

      client.removeObjects(RemoveObjectsArgs.builder().bucket(config.bucket).objects(objects).build()).forEach { r =>
        val e = r.get()
        log.error(s"Failed to delete ${e.objectName}, err ${e.code}: ${e.message}")
        failedKeys.add(e.objectName())
      }

      failedKeys.toSet
    } else {
      Set.empty[String]
    }
  }
}

object ObjectStoreClient {
  val log = com.typesafe.scalalogging.Logger(classOf[ObjectStoreClient])
}
