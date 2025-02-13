
This is a fork of https://github.com/jortage/poolmgr

## Usage

The `--build` ensure that the image is up to date (In case of `Could not connect to 127.0.0.1:3306` error just retry, the DB was just not completely up).

```bash
> docker compose up --build S3DedupProxy
```

This will run three services:

- `S3DedupProxy`: the proxy itself (bind 23278, 23279 and 23290)
- `Postgres`: the database to store the file metadata (binded on 3306)
- `S3Proxy`: used as local S3 store (binded on 8080)

You will need to create the `blobs` bucket in the `S3Proxy` store, since it's run without authentication it can be done with `curl`:

```bash
curl --request PUT http://127.0.0.1:8080/blobs
```

You can then interact with the proxy and store using [MinIO client](https://min.io/docs/minio/linux/reference/minio-mc.html).

```config
{
    "version": "10",
    "aliases": {
        "store": {
            "url": "http://127.0.0.1:80",
            "api": "S3v4",
            "path": "auto"
        },
        "tenant1": {
            "url": "http://127.0.0.1:23278",
            "accessKey": "tenant1",
            "secretKey": "ff54fae017ebe20356223ea016d1e2e867b7f73aaba775fe48ed65d1f1fecb87",
            "api": "S3v4",
            "path": "auto"
        },
        "tenant2": {
            "url": "http://127.0.0.1:23278",
            "accessKey": "tenant2",
            "secretKey": "25ca6d3f04906bfc05a3f2c7ce6b5569ec841ee0ef241a886f34687bbafee7cc",
            "api": "S3v4",
            "path": "auto"
        },
    }
}
```

By default the `test` user must write in the `test` bucket, ex:

```bash
> mc ls store/blobs/
> mc cp file1 tenant1/tenant1/
> mc ls -r store/blobs/
[2025-02-13 22:53:04 CET]   151B STANDARD a/c52/ac52fd97d34cef83527d3b5022775db50b961127ab01b8f646b5040e6f42db02f7d1a6f46bdcd69527d0dbd7d7ee1d92c9681e60b604e2603986516b68541471
> mc cp file1 tenant2/tenant2/
> mc ls -r store/blobs/
[2025-02-13 22:53:04 CET]   151B STANDARD a/c52/ac52fd97d34cef83527d3b5022775db50b961127ab01b8f646b5040e6f42db02f7d1a6f46bdcd69527d0dbd7d7ee1d92c9681e60b604e2603986516b68541471
> mc cp file2 tenant1/tenant1/
> mc ls -r store/blobs/
[2025-02-13 22:55:20 CET]  34KiB STANDARD 8/bb7/8bb7bc575a4a5c18fe537e913f9869bcc016925fdf7c6fbedd3602915cb8341bd609c059f0397f6a42c89bc17baa294f432c3d7983d524d84a8749fd40d1d917
[2025-02-13 22:53:04 CET]   151B STANDARD a/c52/ac52fd97d34cef83527d3b5022775db50b961127ab01b8f646b5040e6f42db02f7d1a6f46bdcd69527d0dbd7d7ee1d92c9681e60b604e2603986516b68541471
```
:warning: For now the bucket name must match the tenant `accessKey`, unsure why will try to remove it :warning:

Have fun

## Remarks

On the strange config you might find:

- `s3.lan`: some part of the code require a domain name and not a host.
- `mastodon.s3.lan`: not sure why but the `bucket` from the config endup as the subdomain of the called url
