//! Storage interfaces and `RocksDB` persistence for Kestrel.

use std::path::Path;

use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch as RocksWriteBatch};
use thiserror::Error;

/// A key/value pair returned by an ordered scan.
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// Storage failures independent of a caller's protocol domain.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("RocksDB operation failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
}

/// A single atomic write operation.
#[derive(Clone, Debug, Eq, PartialEq)]
enum BatchOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// Backend-independent atomic write batch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

impl WriteBatch {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.operations
            .push(BatchOperation::Put(key.into(), value.into()));
        self
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.operations.push(BatchOperation::Delete(key.into()));
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.operations.len()
    }
}

/// Synchronous key/value persistence contract.
///
/// Protocol users should own scheduling and avoid performing these blocking
/// operations directly on an async runtime worker.
pub trait KvStore: Send + Sync {
    /// Reads a value by key.
    ///
    /// # Errors
    ///
    /// Returns a backend error when the read cannot be completed.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// Inserts or replaces a key/value pair.
    ///
    /// # Errors
    ///
    /// Returns a backend error when the write cannot be completed.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;

    /// Deletes a key if it exists.
    ///
    /// # Errors
    ///
    /// Returns a backend error when the deletion cannot be completed.
    fn delete(&self, key: &[u8]) -> Result<(), StorageError>;

    /// Returns matching key/value pairs in lexicographic key order.
    ///
    /// # Errors
    ///
    /// Returns a backend error when iteration cannot be completed.
    fn iterate_prefix(&self, prefix: &[u8]) -> Result<Vec<KeyValue>, StorageError>;

    /// Applies all operations atomically.
    ///
    /// # Errors
    ///
    /// Returns a backend error when the batch cannot be committed.
    fn write_batch(&self, batch: WriteBatch) -> Result<(), StorageError>;
}

/// `RocksDB` implementation of [`KvStore`].
pub struct RocksDbStore {
    database: DB,
}

impl RocksDbStore {
    /// Opens or creates a database at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be opened or created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        Ok(Self {
            database: DB::open(&options, path)?,
        })
    }
}

impl KvStore for RocksDbStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.database.get(key)?)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        Ok(self.database.put(key, value)?)
    }

    fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        Ok(self.database.delete(key)?)
    }

    fn iterate_prefix(&self, prefix: &[u8]) -> Result<Vec<KeyValue>, StorageError> {
        let mut entries = Vec::new();
        for entry in self
            .database
            .iterator(IteratorMode::From(prefix, Direction::Forward))
        {
            let (key, value) = entry?;
            if !key.starts_with(prefix) {
                break;
            }
            entries.push((key.to_vec(), value.to_vec()));
        }
        Ok(entries)
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut rocks_batch = RocksWriteBatch::default();
        for operation in batch.operations {
            match operation {
                BatchOperation::Put(key, value) => rocks_batch.put(key, value),
                BatchOperation::Delete(key) => rocks_batch.delete(key),
            }
        }
        Ok(self.database.write(rocks_batch)?)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{KvStore, RocksDbStore, WriteBatch};

    #[test]
    fn crud_and_ordered_prefix_iteration_work() {
        let directory = TempDir::new().unwrap();
        let store = RocksDbStore::open(directory.path()).unwrap();
        store.put(b"account:2", b"two").unwrap();
        store.put(b"account:1", b"one").unwrap();
        store.put(b"object:1", b"other").unwrap();

        assert_eq!(store.get(b"account:1").unwrap(), Some(b"one".to_vec()));
        assert_eq!(
            store.iterate_prefix(b"account:").unwrap(),
            vec![
                (b"account:1".to_vec(), b"one".to_vec()),
                (b"account:2".to_vec(), b"two".to_vec()),
            ]
        );

        store.delete(b"account:1").unwrap();
        assert_eq!(store.get(b"account:1").unwrap(), None);
    }

    #[test]
    fn batch_is_atomic_and_persists_after_reopen() {
        let directory = TempDir::new().unwrap();
        {
            let store = RocksDbStore::open(directory.path()).unwrap();
            store.put(b"old", b"value").unwrap();
            let mut batch = WriteBatch::new();
            batch.put(b"new", b"value").delete(b"old");
            store.write_batch(batch).unwrap();
        }

        let reopened = RocksDbStore::open(directory.path()).unwrap();
        assert_eq!(reopened.get(b"old").unwrap(), None);
        assert_eq!(reopened.get(b"new").unwrap(), Some(b"value".to_vec()));
    }
}
