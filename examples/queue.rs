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

use k2hash_rust::{K2hash, Queue, QueueBuilder};

fn main() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
    let value = "hello".to_string();
    q.put(&value).expect("Push failed");
    if let Some(value) = q.get() {
        println!("Popped value: {}", value);
    } else {
        println!("Queue is empty");
    }
    assert!(q.qsize() == 0);
    assert!(q.clear());
    assert!(q.close());
    // Example of using QueueBuilder
    let db = K2hash::open_mem().expect("open_mem failed");
    
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
    qb1.put(&value).expect("Push failed");
    if let Some(value) = qb1.get() {
        println!("Popped value: {}", value);
    } else {
        println!("Queue is empty");
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
