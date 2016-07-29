setKeyspace keyspace

schemaChange {
  version '3.0'
  author 'jsanda'
  tags '0.18.x', '0.19.x'
  cql """
ALTER TABLE data WITH COMPRESSION = {'sstable_compression': 'DeflateCompressor'};
"""
}