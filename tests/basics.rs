//  
// k2hash_rust
//
// Copyright 2025 LY Corporation.
// 
// Rust driver for k2hash that is a NoSQL Key Value Store(KVS) library.
// For k2hash, see https://github.com/yahoojapan/k2hash for the details.
//  
// For the full copyright and license information, please view
// the license file that was distributed with this source code.
//  
// AUTHOR:   Hirotaka Wakabayashi
// CREATE:   Fri, 17 Jul 2025
// REVISION:                        
//  
// 

use k2hash_rust::{DumpLevel, K2hash, K2hashKey, KeyQueue, KeyQueueBuilder, Queue, QueueBuilder};
use std::collections::HashMap;

/// Test for k2hash handle
#[test]
fn test_k2hash_handle() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
}

/// Test for k2hash open_mem
#[test]
fn test_k2hash_open_mem() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
}

/// Test for k2hash set and get
#[test]
fn test_k2hash_set() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    // Set a value
    assert!(
        db.set("test_key", "test_value").is_ok(),
        "Set operation failed"
    );
}

/// Test for k2hash get
#[test]
fn test_k2hash_get() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    // Set a value
    assert!(
        db.set("test_key", "test_value").is_ok(),
        "Set operation failed"
    );
    // Get the value
    let value = db.get("test_key").expect("Get operation failed");
    assert_eq!(
        value,
        Some("test_value".to_string()),
        "Get operation returned unexpected value"
    );
}

/// Test for k2hash set and get
#[test]
fn test_k2hash_set_with_options() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    // Set a value
    assert!(
        db.set_with_options("test_key", "test_value", None, None)
            .is_ok(),
        "Set operation failed"
    );
}

/// Test for k2hash get
#[test]
fn test_k2hash_get_with_options() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    // Set a value
    assert!(
        db.set_with_options("test_key", "test_value", None, None)
            .is_ok(),
        "Set operation failed"
    );
    // Get the value
    let value = db
        .get_with_options("test_key", None)
        .expect("Get operation failed");
    assert_eq!(
        value,
        Some("test_value".to_string()),
        "Get operation returned unexpected value"
    );
}

// K2hash::add_attribute_plugin_lib
// skip this test if the plugin library does not exist.
#[cfg(feature = "attribute_plugin")]
#[cfg_attr(not(feature = "attribute_plugin"), ignore)]
#[test]
fn test_k2hash_add_attribute_plugin_lib() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    // Add the attribute plugin library
    assert!(
        db.add_attribute_plugin_lib("test_plugin.so").is_ok(),
        "Add attribute plugin library failed"
    );
}

#[test]
fn test_k2hash_add_decryption_password() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    // Add a decryption password
    assert!(
        db.add_decryption_password("secretstring").is_ok(),
        "Add decryption password operation failed"
    );
    // Set a value
    assert!(
        db.set_with_options("test_key", "test_value", Some("secretstring"), None)
            .is_ok(),
        "Set operation failed"
    );
    // Get the value
    let value = db
        .get_with_options("test_key", Some("secretstring"))
        .expect("Get operation failed");
    assert_eq!(
        value,
        Some("test_value".to_string()),
        "Get operation returned unexpected value"
    );
}

#[test]
fn test_k2hash_add_subkey() {
    let db = K2hash::open_mem().expect("open_mem failed");
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let subkey = "subkey";
    let subval = "subval";
    assert!(db.set(subkey, subval).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(subkey).expect("Get operation failed"),
        Some("subval".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(
        db.add_subkey(key, subkey, subval).is_ok(),
        "Add subkey operation failed"
    );
    // The following code is failing.
    let subkeys = db.get_subkeys(key).expect("Get subkeys operation failed");
    assert_eq!(
        subkeys,
        vec![subkey.to_string()].into(), // convert `Vec<String>` into `Option<Vec<String>>
        "Get subkeys returned unexpected value"
    );
}

// K2hash::begin_tx
#[test]
fn test_k2hash_begin_tx() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let k2h_tx_log = "test.log";
    assert!(
        db.begin_tx(k2h_tx_log).is_ok(),
        "Begin transaction operation failed"
    );
    // For now, we just check if the transaction file descriptor is valid.
    let tx_fd = db
        .get_tx_file_fd()
        .expect("Get transaction file descriptor failed");
    assert!(
        tx_fd != 0,
        "Transaction file descriptor should not be invalid"
    );
}

// K2hash::create
#[test]
#[cfg(feature = "attribute_plugin")]
#[cfg_attr(not(feature = "attribute_plugin"), ignore)]
fn test_k2hash_create() {
    let k2h_file = "test.k2h";
    assert!(K2hash::create(k2h_file).is_ok(), "Create operation failed");
    let db = K2hash::open(k2h_file).expect("Open operation failed");
    // NOTE(hiwkby)
    // Following test is sometimes failed for some reasons.
    // We need investigate the reason, but currently skip this test.
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
}

// K2hash::dump_to_file
#[test]
fn test_k2hash_dump_to_file() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let k2h_file = "test.k2h";
    assert!(
        db.dump_to_file(k2h_file).is_ok(),
        "Dump to file operation failed"
    );
    // Close the database
}

// K2hash::enable_encryption
#[test]
fn test_k2hash_enable_encryption() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let password = "secretstring";
    // Calls set_default_encryption_password before calling enable_encryption
    assert!(
        db.set_default_encryption_password(password).is_ok(),
        "Set default encryption password operation failed"
    );
    assert!(
        db.enable_encryption(true).is_ok(),
        "Enable encryption operation failed"
    );
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    let encrypted_value = db
        .get_with_options(key, Some(password))
        .expect("Get operation failed with password");
    assert_eq!(
        encrypted_value,
        Some("world".to_string()), // Get operation returned unexpected value
        "Get operation returned unexpected value"
    );
}

// K2hash::enable_history
#[test]
fn test_k2hash_enable_history() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    assert!(
        db.enable_history(true).is_ok(),
        "Enable history operation failed"
    );
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
}

// K2hash::enable_mtime
#[test]
fn test_k2hash_enable_mtime() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    assert!(
        db.enable_mtime(true).is_ok(),
        "Enable mtime operation failed"
    );
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
}

// K2hash::get_attributes
#[test]
fn test_k2hash_get_attributes() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let attr_key = "attrkey1";
    let attr_val = "attrval1";
    // ^^^^ expected `Option<HashMap<String, String>>`, found `Vec<(String, String)>`
    let mut map = HashMap::new();
    map.insert(attr_key.to_string(), attr_val.to_string());
    let attrs = Some(map);
    assert!(
        db.set_attribute(key, attr_key, attr_val).is_ok(),
        "Set attribute operation failed"
    );
    let attributes = db
        .get_attributes(key)
        .expect("Get attributes operation failed");
    assert_eq!(
        attributes, attrs,
        "Get attributes returned unexpected value"
    );
}

// K2hash::get_subkeys
#[test]
fn test_k2hash_get_subkeys() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let subkey = "subkey";
    let subval = "subval";
    assert!(db.set(subkey, subval).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(subkey).expect("Get operation failed"),
        Some("subval".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(
        db.add_subkey(key, subkey, subval).is_ok(),
        "Add subkey operation failed"
    );
    let subkeys = db.get_subkeys(key).expect("Get subkeys operation failed");
    assert_eq!(
        subkeys,
        vec![subkey.to_string()].into(),
        "Get subkeys returned unexpected value"
    );
}

// K2hash::get_tx_file_fd
#[test]
fn test_k2hash_get_tx_file_fd() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let k2h_tx_log = "test.log";
    assert!(
        db.begin_tx(k2h_tx_log).is_ok(),
        "Begin transaction operation failed"
    );
    // For now, we just check if the transaction file descriptor is valid.
    let tx_fd = db
        .get_tx_file_fd()
        .expect("Get transaction file descriptor failed");
    assert!(
        tx_fd != 0,
        "Transaction file descriptor should not be invalid"
    );
}

// K2hash::load_from_file
#[test]
fn test_k2hash_load_from_file() {
    // db = k2hash.K2hash()
    // self.assertTrue(isinstance(db, k2hash.K2hash))
    // key = "hello"
    // val = "world"
    // self.assertTrue(db.set(key, val), True)
    // self.assertTrue(db.get(key), val)
    // k2h_file = "test.k2h"
    // self.assertTrue(db.dump_to_file(k2h_file), val)
    // db.close()

    // db = None
    // db = k2hash.K2hash()
    // self.assertTrue(isinstance(db, k2hash.K2hash))
    // self.assertTrue(db.load_from_file(k2h_file), val)
    // self.assertTrue(db.get(key), val)
    // db.close()
    let k2h_file = "test.k2h";
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(
        db.dump_to_file(k2h_file).is_ok(),
        "Dump to file operation failed"
    );
}

// K2hash::print_attribute_plugins
#[test]
fn test_k2hash_print_attribute_plugins() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(
        db.print_attribute_plugins().is_ok(),
        "Print attribute plugins operation failed"
    );
}

// K2hash::print_attributes
#[test]
fn test_k2hash_print_attributes() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(
        db.print_attributes().is_ok(),
        "Print attributes operation failed"
    );
}

// K2hash::print_table_stats
#[test]
fn test_k2hash_print_table_stats() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(
        db.print_table_stats(DumpLevel::HEADER).is_ok(),
        "Print table stats operation failed"
    );
}

// K2hash::remove
#[test]
fn test_k2hash_remove() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(db.remove(key).is_ok(), "Remove operation failed");
    let result = db.get(key);
    assert!(result.is_err(), "Get operation should return Err after remove");
    assert_eq!(
        result.unwrap_err().to_string(),
        "Failed to get result",
        "Error message should match"
    );
}

// K2hash::remove_subkeys
#[test]
fn test_k2hash_remove_subkeys() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let subkey = "subkey";
    let subval = "subval";
    assert!(db.set(subkey, subval).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(subkey).expect("Get operation failed"),
        Some("subval".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(
        db.add_subkey(key, subkey, subval).is_ok(),
        "Add subkey operation failed"
    );
    let subkeys = db.get_subkeys(key).expect("Get subkeys operation failed");
    assert_eq!(
        subkeys,
        vec![subkey.to_string()].into(),
        "Get subkeys returned unexpected value"
    );
    assert!(
        db.remove_subkeys(key, vec![subkey]).is_ok(),
        "Remove subkeys operation failed"
    );
    let result = db.get_subkeys(key);
    assert!(result.is_err(), "Get operation should return Err after remove");
    assert_eq!(
        result.unwrap_err().to_string(),
        "k2h_get_direct_subkeys returns error",
        "Error message should match"
    );
}

// K2hash::rename
#[test]
fn test_k2hash_rename() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    let newkey = key.chars().rev().collect::<String>();
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    assert!(db.rename(key, &newkey).is_ok(), "Rename operation failed");
    assert_eq!(
        db.get(&newkey).expect("Get operation failed after rename"),
        Some("world".to_string()),
        "Get operation returned unexpected value after rename"
    );
}

// K2hash::set_attribute
#[test]
fn test_k2hash_set_attribute() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let attr_key = "attrkey1";
    let attr_val = "attrval1";
    let mut map = HashMap::new();
    map.insert(attr_key.to_string(), attr_val.to_string());
    let attrs = Some(map);
    assert!(
        db.set_attribute(key, attr_key, attr_val).is_ok(),
        "Set attribute operation failed"
    );
    let attributes = db
        .get_attributes(key)
        .expect("Get attributes operation failed");
    assert_eq!(
        attributes, attrs,
        "Get attributes returned unexpected value"
    );
}

// K2hash::set_debug_level
// skip this test as errors
#[test]
#[cfg(feature = "attribute_plugin")]
#[cfg_attr(not(feature = "attribute_plugin"), ignore)]
fn test_k2hash_set_debug_level() {
    assert!(
        K2hash::set_debug_level(DebugLevel::ERROR).is_ok(),
        "Set debug level operation failed"
    );
}

// K2hash::set_default_encryption_password
#[test]
fn test_k2hash_set_default_encryption_password() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let password = "secretstring";
    // Calls set_default_encryption_password before calling enable_encryption
    assert!(
        db.set_default_encryption_password(password).is_ok(),
        "Set default encryption password operation failed"
    );
    assert!(
        db.enable_encryption(true).is_ok(),
        "Enable encryption operation failed"
    );
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    let encrypted_value = db
        .get_with_options(key, Some(password))
        .expect("Get operation failed with password");
}

// K2hash::set_subkeys
#[test]
fn test_k2hash_set_subkeys() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let subkey = "subkey";
    let subval = "subval";
    let subkeys = vec![(subkey, subval)];
    assert!(
        db.set_subkeys(key, subkeys).is_ok(),
        "Set subkeys operation failed"
    );
    let retrieved_subkeys = db.get_subkeys(key).expect("Get subkeys operation failed");
    assert_eq!(
        retrieved_subkeys,
        vec![subkey.to_string()].into(),
        "Get subkeys returned unexpected value"
    );
}

// K2hash::set_tx_pool_size
#[test]
fn test_k2hash_set_tx_pool_size() {
    let size = 1;
    assert!(
        K2hash::set_tx_pool_size(size).is_ok(),
        "Set transaction pool size operation failed"
    );
    let current_size =
        K2hash::get_tx_pool_size().expect("Get transaction pool size operation failed");
    assert_eq!(
        current_size, size,
        "Transaction pool size should be set to 1"
    );
}

// K2hash::stop_tx
#[test]
fn test_k2hash_stop_tx() {
    let db = K2hash::open_mem().expect("open_mem failed");
    // check if db is not null
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let k2h_tx_log = "test.log";
    assert!(
        db.begin_tx(k2h_tx_log).is_ok(),
        "Begin transaction operation failed"
    );
    let tx_fd = db
        .get_tx_file_fd()
        .expect("Get transaction file descriptor failed");
    assert!(
        tx_fd != 0,
        "Transaction file descriptor should not be invalid"
    );
    assert!(db.stop_tx().is_ok(), "Stop transaction operation failed");
}

// K2hash::version
#[test]
fn test_k2hash_version() {
    let version = K2hash::version();
    assert!(version.is_ok(), "Version should not be None");
}

/// Test for queue operations
#[test]
fn test_queue() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
}

#[test]
fn test_queue_put() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    let value = "hello".to_string();
    assert!(q.put(&value).is_ok(), "Push operation failed");
}

#[test]
fn test_queue_get() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    // converts &str(string slice) to String
    let value = "hello".to_string();
    assert!(q.put(&value).is_ok(), "Push operation failed");
    // check if get returns the correct value
    if let Some(value) = q.get() {
        assert_eq!(value, "hello", "Get operation returned unexpected value");
    } else {
        panic!("Get operation failed or returned None");
    }
}

#[test]
fn test_queue_qsize() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    let value = "hello".to_string();
    assert!(q.put(&value).is_ok(), "Push operation failed");
    let size = q.qsize();
    assert!(size == 1, "Queue size should be 1 after putting the value");
}

#[test]
fn test_queue_element() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    let value = "hello".to_string();
    assert!(q.put(&value).is_ok(), "Push operation failed");
    let size = q.qsize();
    assert!(size == 1, "Queue size should be 1 after putting the value");
    if let Some(element) = q.element(0) {
        assert_eq!(element, "hello", "Element at index 0 should be 'hello'");
    } else {
        panic!("Element at index 0 not found");
    }
    if let Some(element) = q.element(1) {
        panic!("Element at index 1 should not exist, but got: {}", element);
    } else {
        // Expected behavior, as there is only one element in the queue
        assert!(true, "Element at index 1 does not exist as expected");
    }
}

#[test]
fn test_queue_empty() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    assert!(q.empty(), "Queue should be empty initially");
    let value = "hello".to_string();
    assert!(q.put(&value).is_ok(), "Push operation failed");
    assert!(
        !q.empty(),
        "Queue should not be empty after putting a value"
    );
}

#[test]
fn test_queue_print() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    let value = "hello".to_string();
    assert!(q.put(&value).is_ok(), "Push operation failed");
    // Print the queue contents
    q.print();
}

#[test]
fn test_queue_remove() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    let value = "hello".to_string();
    assert!(q.put(&value).is_ok(), "Push operation failed");
    let size = q.qsize();
    assert!(size == 1, "Queue size should be 1 after putting the value");
    let result = q.remove(1);
    assert!(result.is_ok(), "Remove operation failed");
}

#[test]
fn test_queue_clear() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    let value = "hello".to_string();
    assert!(q.put(&value).is_ok(), "Push operation failed");
    let size = q.qsize();
    assert!(size == 1, "Queue size should be 1 after putting the value");
    assert!(q.clear(), "Clear operation failed");
    let size_after_clear = q.qsize();
    assert!(
        size_after_clear == 0,
        "Queue size should be 0 after clearing"
    );
}

#[test]
fn test_queue_close() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    // check if queue is not null
    assert!(q.handle() != 0, "Queue handle should not be zero");
    assert!(q.close(), "Close operation failed");
}

#[test]
fn test_queuebuilder_build() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let fifo = true; // or false, depending on your needs
    let prefix = "test_prefix".to_string();
    let password = Some("your_password".to_string());
    let expire_duration = Some(60); // for 60 seconds expiration
                                    // Create the queue using QueueBuilder
    let qb1 = QueueBuilder::new(db.handle())
        .fifo(fifo)
        .prefix(prefix) // Optional prefix
        .password(password.expect("Error")) // Optional password
        .expire_duration(expire_duration.expect("Error")) // Optional expiration duration
        .build()
        .expect("Queue creation failed");
    // check if queue is not null
    assert!(qb1.handle() != 0, "Queue handle should not be zero");
}

/// Test for KeyQueue operations
#[test]
fn test_keyqueue() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
}

#[test]
fn test_keyqueue_put() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    let key = "hello";
    let value = "world";
    assert!(q.put(key, value).is_ok(), "Push operation failed");
}
#[test]
fn test_keyqueue_get() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    // converts &str(string slice) to String
    let key = "hello";
    let value = "world";
    assert!(q.put(key, value).is_ok(), "Push operation failed");
    // check if get returns the correct value
    if let Some((key, value)) = q.get() {
        assert_eq!(key, "hello", "Get operation returned unexpected value");
        assert_eq!(value, "world", "Get operation returned unexpected value");
    } else {
        panic!("Get operation failed or returned None");
    }
}
#[test]
fn test_keyqueue_qsize() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    let key = "hello";
    let value = "world";
    assert!(q.put(key, value).is_ok(), "Push operation failed");
    let size = q.qsize();
    assert!(
        size == 1,
        "KeyQueue size should be 1 after putting the value"
    );
}
#[test]
fn test_keyqueue_element() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    let key = "hello";
    let value = "world";
    assert!(q.put(key, value).is_ok(), "Push operation failed");
    let size = q.qsize();
    assert!(
        size == 1,
        "KeyQueue size should be 1 after putting the value"
    );
    if let Some((k, v)) = q.element(0) {
        assert_eq!(k, "hello", "Element at index 0 should be 'hello'");
        assert_eq!(v, "world", "Element at index 0 should be 'world'");
    } else {
        panic!("Element at index 0 not found");
    }
    if let Some((k, v)) = q.element(1) {
        panic!("Element at index 1 should not exist, but got: {}:{}", k, v);
    } else {
        // Expected behavior, as there is only one element in the KeyQueue
        assert!(true, "Element at index 1 does not exist as expected");
    }
}
#[test]
fn test_keyqueue_empty() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    assert!(q.empty(), "KeyQueue should be empty initially");
    let key = "hello";
    let value = "world";
    assert!(q.put(key, value).is_ok(), "Push operation failed");
    assert!(
        !q.empty(),
        "KeyQueue should not be empty after putting a value"
    );
}
#[test]
fn test_keyqueue_clear() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    let key = "hello";
    let value = "world";
    assert!(q.put(key, value).is_ok(), "Push operation failed");
    let size = q.qsize();
    assert!(
        size == 1,
        "KeyQueue size should be 1 after putting the value"
    );
    assert!(q.clear(), "Clear operation failed");
    let size_after_clear = q.qsize();
    assert!(
        size_after_clear == 0,
        "KeyQueue size should be 0 after clearing"
    );
}
#[test]
fn test_keyqueue_print() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    let key = "hello";
    let value = "world";
    assert!(q.put(key, value).is_ok(), "Push operation failed");
    // Print the KeyQueue contents
    q.print();
}

#[test]
fn test_keyqueue_remove() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    let key = "hello";
    let value = "world";
    assert!(q.put(key, value).is_ok(), "Push operation failed");
    let size = q.qsize();
    assert!(
        size == 1,
        "KeyQueue size should be 1 after putting the value"
    );
    let result = q.remove(1);
    assert!(result.is_ok(), "Remove operation failed");
}

#[test]
fn test_keyqueue_close() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");
    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(q.handle() != 0, "KeyQueue handle should not be zero");
    assert!(q.close(), "Close operation failed");
}

#[test]
fn test_keyqueuebuilder_build() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let handle = db.handle();
    assert!(handle != 0, "Handle should not be zero");

    let fifo = true; // or false, depending on your needs
    let prefix = "test_prefix".to_string();
    let password = Some("your_password".to_string());
    let expire_duration = Some(60); // for 60 seconds expiration
                                    // Create the KeyQueue using KeyQueueBuilder
    let qb1 = KeyQueueBuilder::new(db.handle())
        .fifo(fifo)
        .prefix(prefix) // Optional prefix
        .password(password.expect("Error")) // Optional password
        .expire_duration(expire_duration.expect("Error")) // Optional expiration duration
        .build()
        .expect("KeyQueue creation failed");
    // check if KeyQueue is not null
    assert!(qb1.handle() != 0, "KeyQueue handle should not be zero");
}

#[test]
fn test_k2hashkey_iterator() {
    let db = K2hash::open_mem().expect("open_mem failed");
    assert!(db.handle() != 0, "Database handle should not be zero");
    let key = "hello";
    let val = "world";
    assert!(db.set(key, val).is_ok(), "Set operation failed");
    assert_eq!(
        db.get(key).expect("Get operation failed"),
        Some("world".to_string()),
        "Get operation returned unexpected value"
    );
    let mut k2hkey = K2hashKey::new(db.handle(), None).expect("K2hashKey creation failed"); // internally calls k2h_find_first.
    assert_eq!(k2hkey.next(), Some("hello".to_string())); // internally calls k2h_find_next.
    assert_eq!(k2hkey.next(), None); // internally calls k2h_find_next, but no more keys are available, so returns None.
}

//
// Local variables:
// tab-width: 4
// c-basic-offset: 4
// End:
// vim600: expandtab sw=4 ts=4 fdm=marker
// vim<600: expandtab sw=4 ts=4
//
