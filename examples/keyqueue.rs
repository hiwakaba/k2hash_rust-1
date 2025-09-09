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

use k2hash_rust::{K2hash, KeyQueue, KeyQueueBuilder};

fn main() {
    let db = K2hash::open_mem().expect("open_mem failed");

    let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
    let key = "hello".to_string();
    let value = "world".to_string();
    q.put(&key, &value).expect("Push failed");
    if let Some(value) = q.get() {
        println!("Popped key: {}, value: {}", value.0, value.1);
    } else {
        println!("KeyQueue is empty");
    }
    assert!(q.qsize() == 0);
    assert!(q.clear());
    assert!(q.close());
    // Example of using KeyQueueBuilder
    let db = K2hash::open_mem().expect("open_mem failed");
    
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
    qb1.put(&key, &value).expect("Push failed");
    if let Some(value) = qb1.get() {
        println!("Popped key: {}, value: {}", value.0, value.1);
    } else {
        println!("KeyQueue is empty");
    }
    assert!(qb1.qsize() == 0);
    assert!(qb1.clear());
    assert!(qb1.close());
}

//
// Local variables:
// tab-width: 4
// c-basic-offset: 4
// End:
// vim600: expandtab sw=4 ts=4 fdm=marker
// vim<600: expandtab sw=4 ts=4
//
