//! RocksDB state backend for production use.

mod backend;
mod keyed_store;
mod states;

pub use backend::RocksDBStateBackend;
pub use keyed_store::RocksDBKeyedStateStore;

/// Helper function to construct RocksDB keys.
pub(crate) fn make_rocksdb_key(
    prefix: &str,
    name: &str,
    user_key: &[u8],
    sub_key: Option<&[u8]>,
) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(prefix.as_bytes());
    key.push(0xff);
    key.extend_from_slice(name.as_bytes());
    key.push(0xff);
    key.extend_from_slice(user_key);
    if let Some(sk) = sub_key {
        key.push(0xff);
        key.extend_from_slice(sk);
    }
    key
}
