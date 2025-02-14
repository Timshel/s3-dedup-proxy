package timshel.s3dedupproxy

import cats.effect._
import cats.effect.std.Dispatcher
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
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

import com.jortage.poolmgr.FileReprocessor;
import timshel.s3dedupproxy.Database;

object ProxyBlobStore {
    val NO_DIR_MSG = "Directories are an illusion";
    val NO_BULK_MSG = "Bulk operations are not implemented by Jortage for safety and speed";


    def hashToKey(hc: HashCode): String  = {
        val hash = hc.toString();
        "blobs/"+hash.substring(0, 1)+"/"+hash.substring(1, 4)+"/"+hash;
    }
}

class ProxyBlobStore(
    blobStore: BlobStore,
    identity: String,
    bucket: String,
    db: Database,
    dispatcher: Dispatcher[IO],
) extends ForwardingBlobStore(blobStore) {

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
        val p = getMapKey(container, name).map {
            case Some(key) => delegate().getBlob(bucket, key)
            case None => null
        }
        dispatcher.unsafeRunSync(p)
    }

    override def getBlob(container: String, name: String, getOptions: GetOptions): Blob = {
        val p = getMapKey(container, name).map {
            case Some(key) => delegate().getBlob(bucket, key, getOptions)
            case None => null
        }
        dispatcher.unsafeRunSync(p)
    }

    override def downloadBlob(container: String, name: String, destination: File): Unit = {
        val p = getMapKey(container, name).map {
            case Some(key) => delegate().downloadBlob(bucket, key, destination)
            case None => {}
        }
        dispatcher.unsafeRunSync(p)
    }

    override def downloadBlob(container: String, name: String, destination: File, executor: ExecutorService): Unit = {
        val p = getMapKey(container, name).map {
            case Some(key) => delegate().downloadBlob(bucket, key, destination, executor)
            case None => {}
        }
        dispatcher.unsafeRunSync(p)
    }

    override def streamBlob(container: String, name: String): InputStream = {
        val p = getMapKey(container, name).map {
            case Some(key) => delegate().streamBlob(bucket, key)
            case None => null
        }
        dispatcher.unsafeRunSync(p)
    }

    override def streamBlob(container: String, name: String, executor: ExecutorService): InputStream = {
        val p = getMapKey(container, name).map {
            case Some(key) => delegate().streamBlob(bucket, key, executor)
            case None => null
        }
        dispatcher.unsafeRunSync(p)
    }

    override def getBlobAccess(container: String, name: String): BlobAccess = {
        BlobAccess.PUBLIC_READ;
    }

    override def getContainerAccess(container: String): ContainerAccess = {
        ContainerAccess.PUBLIC_READ;
    }

    override def blobExists(container: String, name: String): Boolean = {
        val p = getMapKey(container, name).map { k => k.isDefined }
        dispatcher.unsafeRunSync(p)
    }

    override def blobMetadata(container: String, name: String): BlobMetadata = {
        val p = getMapKey(container, name).map {
            case Some(key) => delegate().blobMetadata(bucket, key)
            case None => null
        }
        dispatcher.unsafeRunSync(p)
    }

    override def directoryExists(container: String, directory: String): Boolean = {
        throw new UnsupportedOperationException();
    }

    override def getMaximumNumberOfParts(): Int = {
        delegate().getMaximumNumberOfParts();
    }

    override def getMinimumMultipartPartSize(): Long = {
        delegate().getMinimumMultipartPartSize();
    }

    override def getMaximumMultipartPartSize(): Long = {
        delegate().getMaximumMultipartPartSize();
    }

    override def putBlob(container: String, blob: Blob): String = {
        val blobName = blob.getMetadata().getName();
        var tempFile: File = null;

        try {
            tempFile = File.createTempFile("jortage-proxy-", ".dat");
            val contentType = blob.getPayload().getContentMetadata().getContentType();

            val hash = Using(blob.getPayload().openStream()){ is =>
                Using(Files.newOutputStream(Paths.get(tempFile.toURI()))) { fos =>
                    val hos = new HashingOutputStream(Hashing.sha512(), fos);
                    FileReprocessor.reprocess(is, hos);
                    hos.hash();
                }.get
            }.get

            Using(new FilePayload(tempFile)) { payload =>
                val hash_path = ProxyBlobStore.hashToKey(hash);

                payload.getContentMetadata().setContentType(contentType);
                val meta = delegate().blobMetadata(bucket, hash_path);
                if (meta != null) {
                    db.putMappingU(identity, container, blobName, hash);
                    meta.getETag()
                } else {
                    val blob2 = blobBuilder(hash_path)
                            .payload(payload)
                            .userMetadata(blob.getMetadata().getUserMetadata())
                            .build();

                    db.putPendingU(hash);
                    db.putMappingU(identity, container, blobName, hash);
                    db.putMetadataU(hash, tempFile.length());

                    delegate().putBlob(bucket, blob2, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ).multipart());
                }
            }.get
        } catch {
            case e: IOException =>
                e.printStackTrace();
                throw new UncheckedIOException(e);
            case e: Exception =>
                e.printStackTrace();
                throw e;
        } finally {
            if (tempFile != null) tempFile.delete();
        }
    }

    /** javadoc says options are ignored, so we ignore them too
     */
    override def copyBlob(fromContainer: String, fromName: String, toContainer: String, toName: String, options: CopyOptions): String = {
        val hash = db.getMappingHashU(identity, fromContainer, fromName);
        db.putMappingU(identity, toContainer, toName, hash);
        blobMetadata(bucket, ProxyBlobStore.hashToKey(hash)).getETag();
    }

    override def initiateMultipartUpload(container: String, blobMetadata: BlobMetadata, options: PutOptions): MultipartUpload = {
        val mbm = new MutableBlobMetadataImpl(blobMetadata);
        val tempfile = "multitmp/"+identity+"-"+System.currentTimeMillis()+"-"+System.nanoTime();
        mbm.setName(tempfile);
        mbm.getUserMetadata().put("jortage-creator", identity);
        mbm.getUserMetadata().put("jortage-originalname", blobMetadata.getName());
        db.putMultipartU(identity, container, blobMetadata.getName(), tempfile);
        return delegate().initiateMultipartUpload(bucket, mbm, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
    }

    private def mask(mpu: MultipartUpload): MultipartUpload = {
        MultipartUpload.create(bucket, db.getMultipartFileU(identity, mpu.containerName(), mpu.blobName()), mpu.id(), mpu.blobMetadata(), new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
    }

    private def revmask(mpu: MultipartUpload): MultipartUpload = {
        val (bucket, key) = db.getMultipartKeyU(mpu.blobName());
        return MultipartUpload.create(bucket, key, mpu.id(), mpu.blobMetadata(), new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
    }

    override def abortMultipartUpload(mpu: MultipartUpload): Unit = {
        delegate().abortMultipartUpload(mask(mpu));
    }

    override def completeMultipartUpload(orgMpu: MultipartUpload, parts: List[MultipartPart]): String = {
        try {
            val mpu = mask(orgMpu);
            // TODO this is a bit of a hack and isn't very efficient
            var etag = delegate().completeMultipartUpload(mpu, parts);
            Using(delegate().getBlob(mpu.containerName(), mpu.blobName()).getPayload().openStream()) { stream =>
                val counter = new CountingOutputStream(ByteStreams.nullOutputStream());
                val hos = new HashingOutputStream(Hashing.sha512(), counter);
                FileReprocessor.reprocess(stream, hos);
                val hash = hos.hash();
                val path = ProxyBlobStore.hashToKey(hash);
                // we're about to do a bunch of stuff at once
                // sleep so we don't fall afoul of request rate limits
                // (causes intermittent 429s on at least DigitalOcean)
                Thread.sleep(100);
                val meta = delegate().blobMetadata(mpu.containerName(), mpu.blobName());
                val targetMeta = delegate().blobMetadata(bucket, path);
                if (targetMeta == null) {
                    Thread.sleep(100);
                    etag = delegate().copyBlob(mpu.containerName(), mpu.blobName(), bucket, path, CopyOptions.builder().contentMetadata(meta.getContentMetadata()).build());
                    Thread.sleep(100);
                    try {
                        delegate().setBlobAccess(bucket, path, BlobAccess.PUBLIC_READ);
                    } catch {
                        case _:UnsupportedOperationException => {}
                    }
                    db.putPendingU(hash);
                } else {
                    Thread.sleep(100);
                    etag = targetMeta.getETag();
                }
                db.putMappingU(identity, mpu.containerName(), Preconditions.checkNotNull(meta.getUserMetadata().get("jortage-originalname")), hash);
                db.putMetadataU(hash, counter.getCount());
                db.delMultipartU(mpu.blobName());
                Thread.sleep(100);
                delegate().removeBlob(mpu.containerName(), mpu.blobName());
            }
            return etag;
        } catch {
            case e: IOException => throw new UncheckedIOException(e);
            case e: InterruptedException => throw new RuntimeException(e);
        }
    }

    override def uploadMultipartPart(mpu: MultipartUpload, partNumber: Int, payload: Payload): MultipartPart = {
        delegate().uploadMultipartPart(mask(mpu), partNumber, payload);
    }

    override def listMultipartUpload(mpu: MultipartUpload): List[MultipartPart] = {
        delegate().listMultipartUpload(mask(mpu));
    }

    override def listMultipartUploads(container: String): List[MultipartUpload] = {
        val out = Lists.newArrayList[MultipartUpload]();

        delegate().listMultipartUploads(bucket).forEach((mpu) => {
            if (Objects.equal(mpu.blobMetadata().getUserMetadata().get("jortage-creator"), identity)) {
                val revMpu = revmask(mpu);
                if ( container == revMpu.containerName() ) {
                    out.add(revmask(revMpu));
                }
            }
        });

        out;
    }

    override def putBlob(container: String, blob: Blob, putOptions: PutOptions): String = {
        return putBlob(container, blob);
    }

    override def removeBlob(container: String, name: String): Unit = {
        val hc = db.getMappingHashU(identity, container, name);
        if( db.delMappingU(identity, container, name) ){
            val rc = db.countMappingsU(hc);
            if (rc == 0L) {
                val path = ProxyBlobStore.hashToKey(hc);
                delegate().removeBlob(bucket, path);
                db.delMetadataU(hc);
                db.delPendingU(hc);
            }
        }
    }

    override def removeBlobs(container: String, iterable: java.lang.Iterable[String]): Unit = {
        iterable.forEach( (key) => {
            removeBlob(container, key);
        })
    }

    override def listAssignableLocations(): Set[Location] = {
        Collections.singleton(new LocationImpl(LocationScope.PROVIDER, "jort", "jort", null, Collections.emptySet(), Collections.emptyMap()));
    }

    override def createContainerInLocation(location: Location, container: String): Boolean = {
        true;
    }

    override def createContainerInLocation(location: Location, container: String, createContainerOptions: CreateContainerOptions): Boolean = {
        true;
    }

    override def containerExists(container: String): Boolean = true

    override def setContainerAccess(container: String, containerAccess: ContainerAccess): Unit = {}

    override def setBlobAccess(container: String, name: String, access: BlobAccess): Unit =  {}

    override def clearContainer(container: String): Unit = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
    }

    override def clearContainer(container: String, options: ListContainerOptions): Unit = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
    }

    override def deleteContainer(container: String): Unit = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
    }

    override def deleteContainerIfEmpty(container: String): Boolean = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
    }

    override def list(): PageSet[StorageMetadata] = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
    }

    override def list(container: String): PageSet[StorageMetadata] = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
    }

    override def list(container: String, options: ListContainerOptions): PageSet[StorageMetadata] = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_BULK_MSG);
    }

    override def createDirectory(container: String, directory: String) = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_DIR_MSG);
    }

    override def deleteDirectory(container: String, directory: String) = {
        throw new UnsupportedOperationException(ProxyBlobStore.NO_DIR_MSG);
    }

}
