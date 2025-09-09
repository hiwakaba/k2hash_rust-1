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

use k2hash_rust::K2hash;

fn main() {
    let db = K2hash::open_mem().expect("open_mem failed");
    let key = "key";
    let val = "val";
    db.set(key, val);
    let v = db.get(key);
    println!("{:?}", v); // Some("bar")
    let subkey = "subkey";
    let subval = "subval";
    db.add_subkey(key, subkey, subval);
    let subkeys = db.get_subkeys(key);
    let v_subkeys_iter = subkeys.iter();
    for subk in v_subkeys_iter {
        println!("{:?}", subk); // Some("bar")
    }
}

//
// Local variables:
// tab-width: 4
// c-basic-offset: 4
// End:
// vim600: expandtab sw=4 ts=4 fdm=marker
// vim<600: expandtab sw=4 ts=4
//
