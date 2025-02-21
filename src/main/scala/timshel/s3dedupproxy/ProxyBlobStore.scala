package timshel.s3dedupproxy

import cats.effect._
import cats.effect.std.Dispatcher
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.ForwardingBlobStore;
import org.jclouds.domain.internal.LocationImpl;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationScope;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.FilePayload;
import scala.util.Using

import timshel.s3dedupproxy.Database;

object ProxyBlobStore {
  val NO_BULK_MSG = "Bulk operations are not implemented by Jortage for safety and speed";

  def hashToKey(hc: HashCode): String = {
    val hash = hc.toString();
    "blobs/" + hash.substring(0, 1) + "/" + hash.substring(1, 4) + "/" + hash;
  }
}

class ProxyBlobStore(
    bufferStore: BlobStore,
    blobStore: BlobStore,
    identity: String,
    bucket: String,
    db: Database,
    dispatcher: Dispatcher[IO]
) extends ForwardingBlobStore(blobStore) {
  val log = com.typesafe.scalalogging.Logger(classOf[Application])

  def getMapKey(container: String, name: String): IO[Option[String]] = {
    db.getMappingHash(identity, container, name).map { hco =>
      hco.map(ProxyBlobStore.hashToKey)
    }
  }

  override def getContext(): BlobStoreContext = {
    return delegate().getContext();
  }

  override def blobBuilder(name: String): BlobBuilder = {
    delegate().blobBuilder(name);
  }

  override def getBlob(container: String, name: String): Blob = {
    log.debug(s"getBlob($container, $name)")
    val p = getMapKey(container, name).map {
      case Some(key) => delegate().getBlob(bucket, key)
      case None      => null
    }
    dispatcher.unsafeRunSync(p)
  }

  override def getBlob(container: String, name: String, getOptions: GetOptions): Blob = {
    log.debug(s"getBlob($container, $name, $getOptions)")
    val p = getMapKey(container, name).map {
      case Some(key) => delegate().getBlob(bucket, key, getOptions)
      case None      => null
    }
    dispatcher.unsafeRunSync(p)
  }

  override def downloadBlob(container: String, name: String, destination: File): Unit = {
    log.debug(s"downloadBlob($container, $name, $destination)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO(delegate().downloadBlob(bucket, key, destination))
      case None      => IO.pure[Unit](())
    }
    dispatcher.unsafeRunSync(p)
  }

  override def downloadBlob(container: String, name: String, destination: File, executor: ExecutorService): Unit = {
    log.debug(s"downloadBlob($container, $name, $destination, ES)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO(delegate().downloadBlob(bucket, key, destination, executor))
      case None      => IO.pure[Unit](())
    }
    dispatcher.unsafeRunSync(p)
  }

  override def streamBlob(container: String, name: String): InputStream = {
    log.debug(s"streamBlob($container, $name)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO(delegate().streamBlob(bucket, key))
      case None      => IO.pure(null)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def streamBlob(container: String, name: String, executor: ExecutorService): InputStream = {
    log.debug(s"streamBlob($container, $name, ES)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO(delegate().streamBlob(bucket, key, executor))
      case None      => IO.pure(null)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def getBlobAccess(container: String, name: String): BlobAccess = {
    log.debug(s"getBlobAccess($container, $name)")
    BlobAccess.PUBLIC_READ;
  }

  override def getContainerAccess(container: String): ContainerAccess = {
    log.debug(s"getContainerAccess($container)")
    ContainerAccess.PUBLIC_READ;
  }

  override def blobExists(container: String, name: String): Boolean = {
    log.debug(s"blobExists($container, $name)")
    val p = getMapKey(container, name).map { k => k.isDefined }
    dispatcher.unsafeRunSync(p)
  }

  override def blobMetadata(container: String, name: String): BlobMetadata = {
    log.debug(s"blobMetadata($container, $name)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO(delegate().blobMetadata(bucket, key))
      case None      => IO.pure(null)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def directoryExists(container: String, directory: String): Boolean = {
    log.debug(s"directoryExists($container, $directory)")
    true
  }

  override def getMaximumNumberOfParts(): Int = {
    log.debug(s"getMaximumNumberOfParts()")
    delegate().getMaximumNumberOfParts();
  }

  override def getMinimumMultipartPartSize(): Long = {
    log.debug(s"getMinimumMultipartPartSize()")
    delegate().getMinimumMultipartPartSize();
  }

  override def getMaximumMultipartPartSize(): Long = {
    log.debug(s"getMaximumMultipartPartSize()")
    delegate().getMaximumMultipartPartSize();
  }

  private def ensureContainerExists(container: String): IO[Unit] = IO {
    if (!bufferStore.containerExists(container)) {
      bufferStore.createContainerInLocation(null, container)
    }
  }

  override def putBlob(container: String, blob: Blob): String = {
    log.debug(s"putBlob($container, $blob)")
    val name = blob.getMetadata().getName()

    val p = (for {
      _ <- ensureContainerExists(container)
      (size, hash) <- IO {
        val is      = blob.getPayload().openStream();
        val counter = new com.google.common.io.CountingInputStream(is);
        val his     = new com.google.common.hash.HashingInputStream(Hashing.sha512(), counter)
        blob.setPayload(his)
        bufferStore.putBlob(container, blob)
        (counter.getCount(), his.hash())
      }
      eTag <- processBufferDedup(container, name, hash, size)
    } yield eTag)
      .onError { e =>
        IO {
          log.error(s"Failed to putBlob($container, $blob): $e")
        }
      }

    dispatcher.unsafeRunSync(p)
  }

  def processBufferDedup(container: String, name: String, hash: HashCode, size: Long): IO[String] = {
    db.getMetadata(hash)
      .flatMap {
        case Some(metadata) => IO.pure(metadata.eTag)
        case None =>
          for {
            eTag <- IO {
              val blob     = bufferStore.getBlob(container, name)
              val metadata = blob.getMetadata()
              metadata.setContainer(bucket)
              metadata.setName(ProxyBlobStore.hashToKey(hash))
              delegate().putBlob(bucket, blob, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ).multipart());
            }
            _ <- db.putMetadata(hash, size, eTag)
          } yield eTag
      }
      .flatMap { eTag =>
        for {
          _ <- db.putMapping(identity, container, name, hash)
          _ <- IO(bufferStore.removeBlob(container, name))
        } yield eTag
      }
  }

  /** javadoc says options are ignored, so we ignore them too
    */
  override def copyBlob(
      fromContainer: String,
      fromName: String,
      toContainer: String,
      toName: String,
      options: CopyOptions
  ): String = {
    val p = for {
      hash <- db.getMappingHash(identity, fromContainer, fromName).map {
        case Some(hash) => hash
        case None       => throw new IllegalArgumentException("Not found")
      }
      _ <- db.putMapping(identity, toContainer, toName, hash)
      metadata <- db.getMetadata(hash).map {
        case Some(metadata) => metadata
        case None           => throw new IllegalArgumentException("Not found")
      }
    } yield metadata.eTag

    dispatcher.unsafeRunSync(p)
  }

  override def initiateMultipartUpload(container: String, blobMetadata: BlobMetadata, options: PutOptions): MultipartUpload = {
    log.debug(s"initiateMultipartUpload($container, $blobMetadata, $options)")

    val p = for {
      _ <- ensureContainerExists(container)
      mu <- IO(
        bufferStore.initiateMultipartUpload(container, blobMetadata, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ))
      )
    } yield mu

    dispatcher.unsafeRunSync(p)
  }

  override def abortMultipartUpload(mpu: MultipartUpload): Unit = {
    log.debug(s"abortMultipartUpload($mpu)")
    bufferStore.abortMultipartUpload(mpu);
  }

  def bufferStoreBlobHash(container: String, name: String): IO[(Long, HashCode)] = IO {
    Using(bufferStore.getBlob(container, name).getPayload().openStream()) { stream =>
      val counter = new com.google.common.io.CountingOutputStream(java.io.OutputStream.nullOutputStream());
      val hos     = new com.google.common.hash.HashingOutputStream(Hashing.sha512(), counter);
      stream.transferTo(hos)
      (counter.getCount(), hos.hash())
    }.get
  }

  // Not the most efficient since we read the file to compute the size and hash
  override def completeMultipartUpload(mpu: MultipartUpload, parts: java.util.List[MultipartPart]): String = {
    log.debug(s"completeMultipartUpload($mpu, $parts)")

    val container = mpu.containerName()
    val name      = mpu.blobName()

    val p = (for {
      completed <- IO(bufferStore.completeMultipartUpload(mpu, parts))
      _ = log.debug(s"Completed upload to bufferStore: $completed")
      (size, hash) <- bufferStoreBlobHash(container, name)
      eTag         <- processBufferDedup(container, name, hash, size)
    } yield eTag)
      .onError { e =>
        IO {
          log.error(s"Failed to completeMultipartUpload(${mpu.id()}): $e")
        }
      }

    dispatcher.unsafeRunSync(p)
  }

  override def uploadMultipartPart(mpu: MultipartUpload, partNumber: Int, payload: Payload): MultipartPart = {
    log.debug(s"uploadMultipartPart($mpu, $partNumber, $payload)")
    bufferStore.uploadMultipartPart(mpu, partNumber, payload)
  }

  override def listMultipartUpload(mpu: MultipartUpload): java.util.List[MultipartPart] = {
    log.debug(s"listMultipartUpload($mpu)")
    bufferStore.listMultipartUpload(mpu)
  }

  override def listMultipartUploads(container: String): java.util.List[MultipartUpload] = {
    log.debug(s"listMultipartUploads($container)")
    bufferStore.listMultipartUploads(container)
  }

  override def putBlob(container: String, blob: Blob, putOptions: PutOptions): String = {
    return putBlob(container, blob);
  }

  // TODO cleanup will be handled separatly
  override def removeBlob(container: String, name: String): Unit = {
    log.debug(s"removeBlob($container, $name)")
    val p = db.delMapping(identity, container, name)
    dispatcher.unsafeRunSync(p)
  }

  override def removeBlobs(container: String, iterable: java.lang.Iterable[String]): Unit = {
    log.debug(s"removeBlobs($container, $iterable)")
    import scala.jdk.CollectionConverters._
    val p = db.delMappingKeys(identity, container, iterable.asScala.toList)
    dispatcher.unsafeRunSync(p)
  }

  override def listAssignableLocations(): java.util.Set[Location] = {
    log.debug(s"listAssignableLocations()")
    java.util.Collections.emptySet[Location]()
  }

  override def createContainerInLocation(location: Location, container: String): Boolean = {
    log.debug(s"createContainerInLocation($location, $container)")
    true
  }

  override def createContainerInLocation(
      location: Location,
      container: String,
      createContainerOptions: CreateContainerOptions
  ): Boolean = {
    log.debug(s"createContainerInLocation($location, $container, $createContainerOptions)")
    true
  }

  override def containerExists(container: String): Boolean = {
    log.debug(s"containerExists($container)")
    true
  }

  override def setContainerAccess(container: String, containerAccess: ContainerAccess): Unit = {
    log.debug(s"setContainerAccess($container, $containerAccess)")
  }

  override def setBlobAccess(container: String, name: String, access: BlobAccess): Unit = {
    log.debug(s"setBlobAccess($container, $name, $access)")
  }

  override def clearContainer(container: String): Unit = {
    log.debug(s"clearContainer($container)")
    val p = db.delMappings(identity, container)
    dispatcher.unsafeRunSync(p)
  }

  override def clearContainer(container: String, options: ListContainerOptions): Unit = {
    log.debug(s"clearContainer($container, $options)")
    val p = Option(options.getPrefix()) match {
      case Some(prefix) => db.delMappings(identity, container, prefix)
      case None => db.delMappings(identity, container)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def deleteContainer(container: String): Unit = {
    log.debug(s"deleteContainer($container)")
    val p = db.delMappings(identity, container)
    dispatcher.unsafeRunSync(p)
  }

  override def deleteContainerIfEmpty(container: String): Boolean = {
    log.debug(s"deleteContainerIfEmpty($container)")
    val p = db.countMappings(identity, container).map { c => c == 0 }
    dispatcher.unsafeRunSync(p)
  }

  override def list(): PageSet[StorageMetadata] = {
    log.debug(s"Uninplemented list()")
    throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
  }

  override def list(container: String): PageSet[StorageMetadata] = {
    log.debug(s"Uninplemented list($container)")
    throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
  }

  override def list(container: String, options: ListContainerOptions): PageSet[StorageMetadata] = {
    log.debug(s"Uninplemented list($container, $options)")
    throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
  }

  override def createDirectory(container: String, directory: String) = {
    log.debug(s"createDirectory($container, $directory)")
  }

  override def deleteDirectory(container: String, directory: String) = {
    log.debug(s"deleteDirectory($container, $directory)")
  }

}
