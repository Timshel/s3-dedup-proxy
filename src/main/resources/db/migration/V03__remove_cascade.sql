-- Remove ON DELETE CASCADE to prevent cross-tenant data loss.
-- Deleting file_metadata must not cascade-delete file_mappings from other tenants.
ALTER TABLE file_mappings
    DROP CONSTRAINT file_mappings_hash_fkey;

ALTER TABLE file_mappings
    ADD CONSTRAINT file_mappings_hash_fkey FOREIGN KEY (hash) REFERENCES file_metadata(hash) ON UPDATE CASCADE ON DELETE RESTRICT;
