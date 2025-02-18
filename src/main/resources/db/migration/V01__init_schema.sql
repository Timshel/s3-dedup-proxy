CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE file_mappings(
        uuid            UUID NOT NULL DEFAULT uuid_generate_v4(),
        user_name       TEXT NOT NULL,
        bucket          TEXT NOT NULL,
        file_key        TEXT NOT NULL,
        hash            bytea NOT NULL,

        created         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (uuid)
);

CREATE UNIQUE INDEX file_mappings_forward  on file_mappings(user_name, bucket, file_key);
CREATE INDEX file_mappings_reverse  on file_mappings(hash);

CREATE TABLE multipart_uploads(
        uuid            UUID NOT NULL DEFAULT uuid_generate_v4(),
        user_name       TEXT NOT NULL,
        bucket          TEXT NOT NULL,
        file_key        TEXT NOT NULL,
        tempfile        TEXT NOT NULL,

        created         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (uuid)
);
CREATE UNIQUE INDEX multipart_uploads_forward  on multipart_uploads(user_name, bucket, file_key);
CREATE INDEX multipart_uploads_reverse  on multipart_uploads(tempfile);

CREATE TABLE file_metadata(
        hash bytea      NOT NULL,
        size BIGINT     NOT NULL,
        etag TEXT       NOT NULL,

        created         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (hash)
);
