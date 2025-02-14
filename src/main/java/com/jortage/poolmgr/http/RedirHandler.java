package com.jortage.poolmgr.http;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.jclouds.blobstore.domain.Blob;

import com.jortage.poolmgr.Poolmgr;

import com.google.common.base.Splitter;
import com.google.common.hash.HashCode;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;

import timshel.s3dedupproxy.Database;

public final class RedirHandler extends AbstractHandler {
	private static final BaseEncoding B64URLNP = BaseEncoding.base64Url().omitPadding();
	private static final Splitter REDIR_SPLITTER = Splitter.on('/').limit(2).omitEmptyStrings();
	// same regex on the CDN
	private static final Pattern VALID_EXTENSION = Pattern.compile("^(\\.[a-zA-Z0-9.]{2,8})?$");

	private String publicHost;
	private Database db;

	public RedirHandler(String publicHost, Database db) {
		this.publicHost = publicHost;
		this.db = db;
	}


	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		baseRequest.setHandled(true);
		List<String> split = REDIR_SPLITTER.splitToList(target);
		if (split.size() != 3) {
			response.sendError(400);
			return;
		} else {
			String identity = split.get(0);
			String bucket = split.get(0);
			String key = split.get(1);
			try {
				boolean waited = false;
				while (true) {
					Object mutex = null;
					synchronized (Poolmgr.provisionalMaps) {
						mutex = Poolmgr.provisionalMaps.get(identity, bucket + "/" + key);
					}
					if (mutex == null) break;
					waited = true;
					synchronized (mutex) {
						try {
							mutex.wait();
						} catch (InterruptedException e) {}
					}
				}
				if (waited) {
					response.setHeader("Jortage-Waited", "true");
				}
				HashCode hash = db.getMappingHashU(identity, bucket, key);
				if( hash == null ) {
					throw new IllegalArgumentException("Not found");
				}
				response.setHeader("Cache-Control", "public");
				if (Poolmgr.useNewUrls) {
					int dotIdx = key.indexOf('.', key.lastIndexOf('/')+1);
					String ext = "";
					if (dotIdx != -1) {
						ext = key.substring(dotIdx);
					}
					while (!ext.isEmpty() && !VALID_EXTENSION.matcher(ext).matches()) {
						int ind = ext.indexOf('.', 1);
						if (ind == -1) {
							// can't use this extension, drop it
							ext = "";
						} else {
							// reduce the extension until it is valid
							ext = ext.substring(ind);
						}
					}
					String b64 = B64URLNP.encode(hash.asBytes());
					response.setHeader("Location", publicHost+"/blob2/"+b64.substring(0, 16)+"/"+b64.substring(16, b64.length()-8)+"/"+b64.substring(b64.length()-8)+ext);
				} else {
					response.setHeader("Location", publicHost+"/"+Poolmgr.hashToPath(hash));
				}
				response.setStatus(301);
			} catch (IllegalArgumentException e) {
				response.sendError(404);
			}
		}
	}
}
