package com.jortage.poolmgr;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import sun.misc.Signal;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

import com.jortage.poolmgr.http.OuterHandler;
import com.jortage.poolmgr.http.RedirHandler;
import com.jortage.poolmgr.rivet.RivetHandler;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.escape.Escaper;
import com.google.common.hash.HashCode;
import com.google.common.net.UrlEscapers;

import timshel.s3dedupproxy.BackendConfig;
import timshel.s3dedupproxy.Database;
import timshel.s3dedupproxy.GlobalConfig;

public class Poolmgr {

	public static BlobStore backingBlobStore, backingBackupBlobStore;
	public static DataSource dataSource;
	public static Map<String, String> users;
	public static volatile boolean readOnly = false;
	private static boolean backingUp = false;
	public static boolean useNewUrls;
	
	public static final Table<String, String, Object> provisionalMaps = HashBasedTable.create();

	public static void start(GlobalConfig config, Database db) throws Exception {
		try {
			Stopwatch initSw = Stopwatch.createStarted();
			loadConfig(config);
	
			System.err.print("Starting S3 server... ");
			System.err.flush();
			S3Proxy s3Proxy = S3Proxy.builder()
					.awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "DUMMY", "DUMMY")
					.endpoint(URI.create("http://localhost:23278"))
					.jettyMaxThreads(24)
					.v4MaxNonChunkedRequestSize(128L*1024L*1024L)
					// S3Proxy will throw if it sees an X-Amz header it doesn't recognize
					// Misskey, starting in some recent version (as of July 2023) now sends an X-Amz-User-Agent header
					// So without this, Misskey instances can't upload files. Cool thanks
					.ignoreUnknownHeaders(true)
					.build();
			
			// excuse me, this is mine now
			Field serverField = S3Proxy.class.getDeclaredField("server");
			serverField.setAccessible(true);
			Server s3ProxyServer = (Server) serverField.get(s3Proxy);
			s3ProxyServer.setHandler(new OuterHandler(s3ProxyServer.getHandler()));
			QueuedThreadPool pool = (QueuedThreadPool)s3ProxyServer.getThreadPool();
			pool.setName("Jetty-Common");
	
			s3Proxy.setBlobStoreLocator((identity, container, blob) -> {
				String secret = users.get(identity);
				if (secret != null) {
					return Maps.immutableEntry(secret,
							new JortageBlobStore(backingBlobStore, config.backend().bucket(), identity, db));
				} else {
					throw new SecurityException("Access denied");
				}
			});
	
			s3Proxy.start();
			System.err.println("ready on http://localhost:23278");
	
			System.err.print("Starting redirector server... ");
			System.err.flush();
			Server redir = new Server(pool);
			ServerConnector redirConn = new ServerConnector(redir);
			redirConn.setHost("localhost");
			redirConn.setPort(23279);
			redir.addConnector(redirConn);
			redir.setHandler(new OuterHandler(new RedirHandler(config.backend().publicHost(), db)));
			redir.start();
			System.err.println("ready on http://localhost:23279");
			
			if( config.rivet().enabled() ) {
				System.err.print("Starting Rivet server... ");
				System.err.flush();
				Server rivet = new Server(pool);
				ServerConnector rivetConn = new ServerConnector(rivet);
				rivetConn.setHost("localhost");
				rivetConn.setPort(23280);
				rivet.addConnector(rivetConn);
				rivet.setHandler(new OuterHandler(new RivetHandler(config.backend().bucket(), config.backend().publicHost(), db)));
				rivet.start();
				System.err.println("ready on http://localhost:23280");
			} else {
				System.err.println("Not starting Rivet server.");
			}
			
			System.err.print("Registering SIGALRM handler for backups... ");
			System.err.flush();
			try {
				Signal.handle(new Signal("ALRM"), (sig) -> {
					if (backingUp) {
						System.err.println("Ignoring SIGALRM, backup already in progress");
						return;
					}
					if (backingBackupBlobStore == null) {
						System.err.println("Ignoring SIGALRM, nowhere to backup to");
						return;
					}
					new Thread(() -> {
						int count = 0;
						Stopwatch sw = Stopwatch.createStarted();
						try (Connection c = dataSource.getConnection()) {
							backingUp = true;
							String bucket = config.backend().bucket();
							String backupBucket = config.backupBackend().get().bucket();

							try (PreparedStatement delete = c.prepareStatement("DELETE FROM `pending_backup` WHERE `hash` = ?;")) {
								try (PreparedStatement ps = c.prepareStatement("SELECT `hash` FROM `pending_backup`;")) {
									try (ResultSet rs = ps.executeQuery()) {
										while (rs.next()) {
											byte[] bys = rs.getBytes("hash");
											String path = hashToPath(HashCode.fromBytes(bys));
											Blob src = backingBlobStore.getBlob(bucket, path);
											if (src == null) {
												Blob actualSrc = backingBackupBlobStore.getBlob(backupBucket, path);
												if (actualSrc == null) {
													System.err.println("Can't find blob "+path+" in source or destination?");
													continue;
												}  else {
													System.err.println("Copying "+path+" from \"backup\" to current - this is a little odd");
													backingBlobStore.putBlob(bucket, actualSrc, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
												}
											} else {
												backingBackupBlobStore.putBlob(backupBucket, src, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
											}
											delete.setBytes(1, bys);
											delete.executeUpdate();
											count++;
										}
										System.err.println("Backup of "+count+" item"+s(count)+" successful in "+sw);
									}
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
							System.err.println("Backup failed after "+count+" item"+s(count)+" in "+sw);
						} finally {
							backingUp = false;
						}
					}, "Backup thread").start();
				});
				System.err.println("done");
			} catch (Exception e) {
				System.err.println("failed");
			}
			System.err.println("This Poolmgr has Super Denim Powers. (Done in "+initSw+")");
		} catch (Throwable t) {
			System.err.println(" failed");
			t.printStackTrace();
		}
	}

	private static String s(int i) {
		return i == 1 ? "" : "s";
	}

	private static void loadConfig(GlobalConfig config) {
		try {
			System.err.print("Constructing blob stores...");
			System.err.flush();
			BlobStore backingBlobStoreTmp = createBlobStore(config.backend());
			BlobStore backingBackupBlobStoreTmp;
			if ( config.backupBackend().nonEmpty() ) {
				backingBackupBlobStoreTmp = createBlobStore(config.backupBackend().get());
			} else {
				backingBackupBlobStoreTmp = null;
			}

			users = scala.collection.JavaConverters.mapAsJavaMapConverter(config.users()).asJava();
			backingBlobStore = backingBlobStoreTmp;
			backingBackupBlobStore = backingBackupBlobStoreTmp;
			readOnly = config.readOnly();
		} catch (Exception e) {
			System.err.println(" failed");
			e.printStackTrace();
		}
	}

	private static BlobStore createBlobStore(BackendConfig conf) {
		String protocol = conf.protocol();
		if ("s3".equals(protocol)) protocol = "aws-s3";
		return ContextBuilder.newBuilder(protocol)
			.credentials(conf.accessKeyId(), conf.secretAccessKey())
			.modules(ImmutableList.of(new SLF4JLoggingModule()))
			.endpoint(conf.endpoint())
			.build(BlobStoreContext.class)
			.getBlobStore();
	}

	public static String hashToPath(HashCode hc) {
		String hash = hc.toString();
		return "blobs/"+hash.substring(0, 1)+"/"+hash.substring(1, 4)+"/"+hash;
	}

	public static void checkReadOnly() {
		if (readOnly) throw new IllegalStateException("Currently in read-only maintenance mode; try again later");
	}

}
