CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE file_metadata(
        hash            bytea NOT NULL,
        md5             bytea NOT NULL,
        size            BIGINT NOT NULL,
        etag            TEXT NOT NULL,
        content_type    TEXT NOT NULL,

        created         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (hash)
);

CREATE TABLE file_mappings(
        uuid            UUID NOT NULL DEFAULT uuid_generate_v4(),
        user_name       TEXT NOT NULL,
        bucket          TEXT NOT NULL,
        file_key        TEXT NOT NULL,
        hash            bytea NOT NULL,

        created         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (uuid),
        FOREIGN KEY(hash) REFERENCES file_metadata(hash) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE UNIQUE INDEX file_mappings_forward  on file_mappings(user_name, bucket, file_key);
CREATE INDEX file_mappings_reverse  on file_mappings(hash);
