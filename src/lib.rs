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

/*!
k2hash_rust is a NoSQL Key Value Store(KVS) library developed by [AntPickax](https://antpick.ax/),
which is an open source team in [LY Corporation](https://www.lycorp.co.jp/en/company/overview/) and a product family
of open source software developed by [AntPickax](https://antpick.ax/).

# k2hash_rust

## Overview

**k2hash_rust** implements a [k2hash](https://k2hash.antpick.ax/) client in Rust.

## Install

Firstly you must install the [k2hash](https://k2hash.antpick.ax/) shared library.
```sh
curl -o- https://raw.github.com/yahoojapan/k2hash_rust/main/utils/libk2hash.sh | bash
```
You can install **k2hash** library step by step from [source code](https://github.com/yahoojapan/k2hash). See [Build](https://k2hash.antpick.ax/build.html) for details.

Download the **k2hash_rust** package.

```sh
cd /path/to/your/rust/project
cargo add k2hash_rust
```

## Usage

Here is a simple example of **k2hash_rust** that saves a key and get it.

```rust
use k2hash_rust::K2hash;

fn main() {
    let db = K2hash::open_mem().expect("open_mem failed");
    db.set("foo", "bar");
    let v = db.get("foo");
    println!("foo => {:?}", v);
}
```

## Development

Here is the step to start developing **k2hash_rust** on Fedora42.

```sh
sudo dnf update -y
```

```sh
sudo dnf makecache && sudo yum install curl git -y && curl -s https://packagecloud.io/install/repositories/antpickax/stable/script.rpm.sh | sudo bash
sudo dnf install libfullock-devel k2hash-devel -y
git clone https://github.com/yahoojapan/k2hash_rust.git
cd k2hash_rust
cargo build
cargo test
```

### Documents
  - [About K2HASH](https://k2hash.antpick.ax/)
  - [About AntPickax](https://antpick.ax/)

### License

MIT License. See the LICENSE file.

## AntPickax

[AntPickax](https://antpick.ax/) is
  - an open source team in [LY Corporation](https://www.lycorp.co.jp/en/company/overview/).
  - a product family of open source software developed by [AntPickax](https://antpick.ax/).
*/

use std::collections::HashMap; // Import HashMap for attributes

// CString:    create CString instance from Rust string.
// CStr:       create CStr instance from C API's pointer.
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uchar, c_ulong, c_ulonglong, c_void};
use std::ptr;

// use libc::size_t;

/// DumpLevel represents the level of detail in the dump output.
#[derive(PartialEq)]
pub enum DumpLevel {
    HEADER,
    HASHTABLE,
    SUBHASHTABLE,
    ELEMENT,
    PAGE,
}

/// DebugLevel represents the level of detail in the debug output.
pub enum DebugLevel {
    SILENT,
    ERROR,
    WARNING,
    MESSAGE,
}

//
// k2hash C API
//
// # typedef struct k2h_key_pack{
// # 	unsigned char*	pkey;
// # 	size_t			length;
// # }K2HKEYPCK, *PK2HKEYPCK;
// #

/// K2hKeyPack represents C-API's K2HKEYPCK structure.
#[repr(C)]
pub struct K2hKeyPack {
    pub pkey: *mut u8,
    pub length: usize,
}
// typedef struct {
//     unsigned char* pkey;
//     size_t keylength;
//     unsigned char* pval;
//     size_t vallength;
// } K2HATTRPCK;

/// K2hAttrPack represents C-API's K2HATTRPCK structure.
#[repr(C)]
pub struct K2hAttrPack {
    pub pkey: *mut u8,
    pub keylength: usize,
    pub pval: *mut u8,
    pub vallength: usize,
}

// K2H_INVALID_HANDLE = 0;

#[link(name = "k2hash")]
extern "C" {

    // k2h_open_mem(int maskbitcnt, int cmaskbitcnt, int maxelementcnt, int pagesize)
    fn k2h_open_mem(maskbitcnt: i32, cmaskbitcnt: i32, maxelementcnt: i32, pagesize: i32) -> u64;

    // bool k2h_set_str_value_wa(k2h_h handle, const char* pkey, const char* pval, const char* pass, const time_t* expire)
    fn k2h_set_str_value_wa(
        handle: u64,
        pkey: *const c_char,
        pval: *const c_char,
        pass: *const c_char,
        expire: *const c_ulonglong,
    ) -> bool;

    // char* k2h_get_str_direct_value_wp(k2h_h handle, const char* pkey, const char* pass)
    fn k2h_get_str_direct_value_wp(
        handle: u64,
        pkey: *const c_char,
        pass: *const c_char,
    ) -> *mut c_char;

    /// Defines prototypes for Rust code
    ////////////////////////////////////////////////////////
    /// 4. find API
    ////////////////////////////////////////////////////////
    ///
    /// k2h_find_h k2h_find_first(k2h_h handle)
    /// k2h_find_first: Find the first key
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    ///
    /// # Returns
    /// * `u64` - find handle
    fn k2h_find_first(handle: u64) -> u64;

    /// k2h_find_first_str_subkey: Find the first subkey for a key
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key string
    ///
    /// # Returns
    /// * `u64` - find handle
    fn k2h_find_first_str_subkey(handle: u64, pkey: *const c_char) -> u64;

    /// k2h_find_next: Find the next key
    ///
    /// # Arguments
    /// * `findhandle` - find handle
    ///
    /// # Returns
    /// * `u64` - next find handle
    fn k2h_find_next(findhandle: u64) -> u64;

    /// k2h_find_get_key: Get key from find handle
    ///
    /// # Arguments
    /// * `findhandle` - find handle
    /// * `ppkey` - pointer to key pointer
    /// * `pkeylength` - pointer to key length
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_find_get_key(findhandle: u64, ppkey: *mut *mut c_uchar, pkeylength: *mut usize) -> bool;

    ////////////////////////////////////////////////////////
    /// 3. keyqueue API
    ////////////////////////////////////////////////////////
    ///
    /// k2h_keyq_handle_str_prefix: Create a key queue handle with prefix
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `is_fifo` - FIFO flag
    /// * `pref` - prefix string
    ///
    /// # Returns
    /// * `u64` - key queue handle
    fn k2h_keyq_handle_str_prefix(handle: u64, is_fifo: bool, pref: *const c_char) -> u64;

    /// k2h_keyq_str_push_keyval: Push a key-value pair into the key queue
    ///
    /// # Arguments
    /// * `keyqhandle` - key queue handle
    /// * `pkey` - key string
    /// * `pval` - value string
    ///
    /// # Returns
    /// * `bool` - true on success
    // fn k2h_keyq_str_push_keyval(keyqhandle: u64, pkey: *const c_char, pval: *const c_char) -> bool;

    /// k2h_keyq_str_push_keyval_wa: Push a key-value pair into the key queue with password and expiration
    ///
    /// # Arguments
    /// * `keyqhandle` - key queue handle
    /// * `pkey` - key string
    /// * `pval` - value string
    /// * `encpass` - encryption password string
    /// * `expire` - pointer to expiration time
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_keyq_str_push_keyval_wa(
        keyqhandle: u64,
        pkey: *const c_char,
        pval: *const c_char,
        encpass: *const c_char,
        expire: *const c_ulonglong,
    ) -> bool;

    /// k2h_keyq_dump: Dump key queue contents to a stream
    ///
    /// # Arguments
    /// * `qhandle` - key queue handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_keyq_dump(qhandle: u64, stream: *mut c_void) -> bool;

    /// k2h_keyq_free: Free a key queue handle
    ///
    /// # Arguments
    /// * `qhandle` - key queue handle
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_keyq_free(qhandle: u64) -> bool;

    /// k2h_keyq_count: Get the number of elements in the key queue
    ///
    /// # Arguments
    /// * `qhandle` - key queue handle
    ///
    /// # Returns
    /// * `c_int` - number of elements
    fn k2h_keyq_count(qhandle: u64) -> c_int;

    /// k2h_keyq_str_read_keyval_wp: Read a key-value pair from the key queue at a specific position with password
    ///
    /// # Arguments
    /// * `keyqhandle` - key queue handle
    /// * `ppkey` - pointer to key pointer
    /// * `ppval` - pointer to value pointer
    /// * `pos` - position in queue
    /// * `encpass` - encryption password string
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_keyq_str_read_keyval_wp(
        keyqhandle: u64,
        ppkey: *mut *mut c_char,
        ppval: *mut *mut c_char,
        pos: c_int,
        encpass: *const c_char,
    ) -> bool;

    /// k2h_keyq_empty: Check if the key queue is empty
    ///
    /// # Arguments
    /// * `qhandle` - key queue handle
    ///
    /// # Returns
    /// * `bool` - true if empty
    fn k2h_keyq_empty(qhandle: u64) -> bool;

    /// k2h_keyq_str_pop_keyval_wp: Pop a key-value pair from the key queue with password
    ///
    /// # Arguments
    /// * `keyqhandle` - key queue handle
    /// * `ppkey` - pointer to key pointer
    /// * `ppval` - pointer to value pointer
    /// * `encpass` - encryption password string
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_keyq_str_pop_keyval_wp(
        keyqhandle: u64,
        ppkey: *mut *mut c_char,
        ppval: *mut *mut c_char,
        encpass: *const c_char,
    ) -> bool;

    /// k2h_keyq_remove: Remove elements from the key queue
    ///
    /// # Arguments
    /// * `qhandle` - key queue handle
    /// * `count` - number of elements to remove
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_keyq_remove(qhandle: u64, count: c_int) -> bool;

    ////////////////////////////////////////////////////////
    /// 2. queue API
    ////////////////////////////////////////////////////////
    /// k2h_q_h k2h_q_handle_str_prefix(k2h_h handle, bool is_fifo, const char* pref)
    /// k2h_q_handle_str_prefix: Create a queue handle with prefix
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `is_fifo` - FIFO flag
    /// * `pref` - prefix string
    ///
    /// # Returns
    /// * `u64` - queue handle
    fn k2h_q_handle_str_prefix(handle: u64, is_fifo: bool, pref: *const c_char) -> u64;

    /// k2h_q_str_push_wa: Push a value with attributes, password, and expiration into the queue
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    /// * `pdata` - data string
    /// * `pattrspck` - pointer to attribute pack
    /// * `attrspckcnt` - attribute pack count
    /// * `encpass` - encryption password string
    /// * `expire` - pointer to expiration time
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_q_str_push_wa(
        qhandle: u64,
        pdata: *const c_char,
        pattrspck: *const c_void,
        attrspckcnt: c_int,
        encpass: *const c_char,
        expire: *const c_ulonglong,
    ) -> bool;

    /// k2h_q_str_push: Push a value into the queue
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    /// * `pval` - value string
    ///
    /// # Returns
    /// * `bool` - true on success
    /// fn k2h_q_str_push(qhandle: u64, pval: *const c_char) -> bool;

    /// k2h_q_remove: Remove elements from the queue
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    /// * `count` - number of elements to remove
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_q_remove(qhandle: u64, count: c_int) -> bool;

    /// k2h_q_free: Free a queue handle
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_q_free(qhandle: u64) -> bool;

    /// k2h_q_count: Get the number of elements in the queue
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    ///
    /// # Returns
    /// * `c_int` - number of elements
    fn k2h_q_count(qhandle: u64) -> c_int;

    /// k2h_q_str_read_wp: Read a value from the queue at a specific position with password
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    /// * `ppdata` - pointer to data pointer
    /// * `pos` - position in queue
    /// * `encpass` - encryption password string
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_q_str_read_wp(
        qhandle: u64,
        ppdata: *mut *mut c_char,
        pos: c_int,
        encpass: *const c_char,
    ) -> bool;

    /// k2h_q_empty: Check if the queue is empty
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    ///
    /// # Returns
    /// * `bool` - true if empty
    fn k2h_q_empty(qhandle: u64) -> bool;

    /// k2h_q_str_pop: Pop a value from the queue
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    /// * `ppval` - pointer to value pointer
    ///
    /// # Returns
    /// * `bool` - true on success
    /// fn k2h_q_str_pop(qhandle: u64, ppval: *mut *mut c_char) -> bool;

    /// k2h_q_str_pop_wp: Pop a value from the queue with password
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    /// * `ppdata` - pointer to data pointer
    /// * `encpass` - encryption password string
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_q_str_pop_wp(qhandle: u64, ppdata: *mut *mut c_char, encpass: *const c_char) -> bool;

    /// k2h_q_dump: Dump queue contents to a stream
    ///
    /// # Arguments
    /// * `qhandle` - queue handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_q_dump(qhandle: u64, stream: *mut c_void) -> bool;

    ////////////////////////////////////////////////////////
    /// 1. k2hash API
    ////////////////////////////////////////////////////////
    /// k2h_add_attr_crypt_pass: Add password for encryption
    ///
    /// # Arguments
    /// * `handle` - k2hash handler
    /// * `pass` - password string
    /// * `is_default_encrypt` - whether to use default encryption
    ///
    /// # Returns
    /// * `bool` - True on success
    /// # bool k2h_add_attr_crypt_pass(k2h_h handle, const char* pass, bool is_default_encrypt)
    fn k2h_add_attr_crypt_pass(handle: u64, pass: *const c_char, is_default_encrypt: bool) -> bool;

    /// # add attr plugin API
    /// # bool k2h_add_attr_plugin_library(k2h_h handle, const char* libpath)
    /// k2h_add_attr_plugin_library: Add attribute plugin library
    ///
    /// # Arguments
    /// * `handle` - k2hash handler
    /// * `libpath` - library path string
    ///
    /// # Returns
    /// * `bool` -  True on success
    fn k2h_add_attr_plugin_library(handle: u64, libpath: *const c_char) -> bool;

    /// # add attr API
    /// # bool k2h_add_str_attr(k2h_h handle, const char* pkey, const char* pattrkey, const char* pattrval)
    /// k2h_add_str_attr: Add string attribute
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key string
    /// * `pattrkey` - attribute key string
    /// * `pattrval` - attribute value string
    ///
    /// # Returns
    /// * `bool` -  True on success
    // fn k2h_add_str_attr(
    //     handle: u64,
    //     pkey: *const c_char,
    //     pattrkey: *const c_char,
    //     pattrval: *const c_char,
    // ) -> bool;

    /// # bool k2h_add_attr(k2h_h handle, const unsigned char* pkey, size_t keylength, const unsigned char* pattrkey, size_t attrkeylength, const unsigned char* pattrval, size_t attrvallength)
    /// k2h_add_attr: Add attribute
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key
    /// * `keylength` - key length
    /// * `pattrkey` - attribute key
    /// * `attrkeylength` - attribute key length
    /// * `pattrval` - attribute value
    /// * `attrvallength` - attribute value length
    ///
    /// # Returns
    /// * `bool` -  True on success
    fn k2h_add_attr(
        handle: u64,
        pkey: *const u8,
        keylength: usize,
        pattrkey: *const u8,
        attrkeylength: usize,
        pattrval: *const u8,
        attrvallength: usize,
    ) -> bool;

    /// # add subkey API
    /// # bool k2h_add_subkey(k2h_h handle, const unsigned char* pkey, size_t keylength, const unsigned char* psubkey, size_t skeylength, const unsigned char* pval, size_t vallength)
    /// k2h_add_subkey: Add a subkey attribute
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key pointer
    /// * `keylength` - key length
    /// * `psubkey` - subkey pointer
    /// * `skeylength` - subkey length
    /// * `pval` - value pointer
    /// * `vallength` - value length
    ///
    /// # Returns
    /// * `bool` - true on success
    // fn k2h_add_subkey(
    //     handle: u64,
    //     pkey: *const u8,
    //     keylength: usize,
    //     psubkey: *const u8,
    //     skeylength: usize,
    //     pval: *const u8,
    //     vallength: usize,
    // ) -> bool;

    /// # bool k2h_add_subkey_wa(k2h_h handle, const unsigned char* pkey, size_t keylength, const unsigned char* psubkey, size_t skeylength, const unsigned char* pval, size_t vallength, const char* pass, const time_t* expire)
    /// k2h_add_subkey_wa: Add subkey attribute（encryption, expiration in second）
    ///
    /// # Arguments
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key
    /// * `keylength` - key length
    /// * `psubkey` - subkey
    /// * `skeylength` - subkey length
    /// * `pval` - valuea
    /// * `vallength` - value length
    /// * `pass` - password
    /// * `expire` - expiration in second
    ///
    /// # Returns
    /// * `bool` - True on success
    fn k2h_add_subkey_wa(
        handle: u64,
        pkey: *const u8,
        keylength: usize,
        psubkey: *const u8,
        skeylength: usize,
        pval: *const u8,
        vallength: usize,
        pass: *const c_char,
        expire: *const c_ulonglong,
    ) -> bool;

    /// # close API
    /// # bool k2h_close(k2h_h handle)
    /// k2h_close: close k2hash handle
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    ///
    /// # Returns
    /// * `bool` - True on success
    fn k2h_close(handle: u64) -> bool;

    /// # bool k2h_close_wait(k2h_h handle, long waitms)
    /// k2h_close_wait: close k2hash handle with wait
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `waitms` - wait time in milliseconds
    ///
    /// # Returns
    /// * `bool` - True on success
    // fn k2h_close_wait(handle: u64, waitms: i64) -> bool;

    /// # create API
    /// # bool k2h_create(const char* filepath, int maskbitcnt, int cmaskbitcnt, int maxelementcnt, size_t pagesize)
    /// k2h_create: Create a new k2hash database
    ///
    /// # Arguments
    /// * `filepath` - file path for the database
    /// * `maskbitcnt` - mask bit count
    /// * `cmaskbitcnt` - chain mask bit count
    /// * `maxelementcnt` - maximum element count
    /// * `pagesize` - page size
    ///
    /// # Returns
    /// * `bool` - True on success
    fn k2h_create(
        filepath: *const c_char,
        maskbitcnt: i32,
        cmaskbitcnt: i32,
        maxelementcnt: i32,
        pagesize: usize,
    ) -> bool;

    /// # disable tx API
    /// # bool k2h_disable_transaction(k2h_h handle)
    /// k2h_disable_transaction: disable transaction
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    ///
    /// # Returns
    /// * `bool` - True on success
    fn k2h_disable_transaction(handle: u64) -> bool;

    /// # dump API
    /// # bool k2h_dump_head(k2h_h handle, FILE* stream)
    /// k2h_dump_head: header information dump
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - True on success    
    fn k2h_dump_head(handle: u64, stream: *mut c_void) -> bool;

    /// # bool k2h_dump_keytable(k2h_h handle, FILE* stream)
    /// k2h_dump_keytable: dump key table information
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - True on success
    fn k2h_dump_keytable(handle: u64, stream: *mut c_void) -> bool;

    /// # bool k2h_dump_full_keytable(k2h_h handle, FILE* stream)
    /// k2h_dump_full_keytable: dump full key table information
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - True on success
    fn k2h_dump_full_keytable(handle: u64, stream: *mut c_void) -> bool;

    /// # bool k2h_dump_elementtable(k2h_h handle, FILE* stream)
    /// k2h_dump_elementtable: dump element table information
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - True on success
    fn k2h_dump_elementtable(handle: u64, stream: *mut c_void) -> bool;

    /// # bool k2h_dump_full(k2h_h handle, FILE* stream)
    /// k2h_dump_full: dump full k2hash information
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - True on success
    fn k2h_dump_full(handle: u64, stream: *mut c_void) -> bool;

    /// # get value API
    /// # char* k2h_get_str_direct_value_wp(k2h_h handle, const char* pkey, const char* pass)
    /// k2h_get_str_direct_value_wp: Get string value directly
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key string
    /// * `pass` - password string
    ///
    /// # Returns
    /// * `*mut c_char` - pointer to C string (must be freed if necessary)
    /// fn k2h_get_str_direct_value_wp(
    ///     handle: u64,
    ///     pkey: *const c_char,
    ///     pass: *const c_char,
    /// ) -> *mut c_char;

    /// # get attrs API
    /// # PK2HATTRPCK k2h_get_direct_attrs(k2h_h handle, const unsigned char* pkey, size_t keylength, int* pattrspckcnt)
    /// k2h_get_direct_attrs: Get direct attributes
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key pointer
    /// * `keylength` - key length
    /// * `pattrspckcnt` - pointer to attribute count
    ///
    /// # Returns
    /// * `*mut c_void` - pointer to attribute pack (replace with actual struct if available)
    fn k2h_get_direct_attrs(
        handle: u64,
        pkey: *const u8,
        keylength: usize,
        pattrspckcnt: *mut c_int,
    ) -> *mut c_void;

    /// # get subkeys API
    /// # PK2HKEYPCK k2h_get_direct_subkeys(k2h_h handle, const unsigned char* pkey, size_t keylength, int* pskeypckcnt)
    /// k2h_get_direct_subkeys: Get direct subkeys
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key pointer
    /// * `keylength` - key length
    /// * `pskeypckcnt` - pointer to subkey count
    ///
    /// # Returns
    /// * `*mut c_void` - pointer to subkey pack (replace with actual struct if available)
    fn k2h_get_direct_subkeys(
        handle: u64,
        pkey: *const u8,
        keylength: usize,
        pskeypckcnt: *mut c_int,
    ) -> *mut c_void;

    /// # get transaction API
    /// # int k2h_get_transaction_archive_fd(k2h_h handle)
    /// k2h_get_transaction_archive_fd: Get transaction archive file descriptor
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    ///
    /// # Returns
    /// * `c_int` - file descriptor
    fn k2h_get_transaction_archive_fd(handle: u64) -> c_int;

    /// # int k2h_get_transaction_thread_pool(void)
    /// k2h_get_transaction_thread_pool: Get transaction thread pool count
    ///
    /// # Arguments
    /// (none)
    ///
    /// # Returns
    /// * `c_int` - thread pool count
    fn k2h_get_transaction_thread_pool() -> c_int;

    /// # load archive API
    /// # bool k2h_load_archive(k2h_h handle, const char* filepath, bool errskip)
    /// k2h_load_archive: Load archive file
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `filepath` - file path
    /// * `errskip` - skip errors flag
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_load_archive(handle: u64, filepath: *const c_char, errskip: bool) -> bool;

    /// # open API
    // k2h_open_mem(int maskbitcnt, int cmaskbitcnt, int maxelementcnt, int pagesize)
    fn k2h_open(
        pfile: *const c_char,
        readonly: bool,
        removefile: bool,
        fullmap: bool,
        maskbitcnt: i32,
        cmaskbitcnt: i32,
        maxelementcnt: i32,
        pagesize: i32,
    ) -> u64;

    /// # print API
    /// # bool k2h_print_attr_version(k2h_h handle, FILE* stream)
    /// k2h_print_attr_version: Print attribute version information
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_print_attr_version(handle: u64, stream: *mut c_void) -> bool;

    /// # bool k2h_print_attr_information(k2h_h handle, FILE* stream)
    /// k2h_print_attr_information: Print attribute information
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_print_attr_information(handle: u64, stream: *mut c_void) -> bool;

    /// # bool k2h_print_state(k2h_h handle, FILE* stream)
    /// k2h_print_state: Print state information
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_print_state(handle: u64, stream: *mut c_void) -> bool;

    /// # void k2h_print_version(FILE* stream)
    /// k2h_print_version: Print version information
    ///
    /// # Arguments
    /// * `stream` - FILE pointer
    ///
    /// # Returns
    /// * nothing (void)
    fn k2h_print_version(stream: *mut c_void);

    /// # put_archive API
    /// # bool k2h_put_archive(k2h_h handle, const char* filepath, bool errskip)
    /// k2h_put_archive: Put archive file
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `filepath` - file path
    /// * `errskip` - skip errors flag
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_put_archive(handle: u64, filepath: *const c_char, errskip: bool) -> bool;

    /// # remove API
    /// # bool k2h_remove_str_all(k2h_h handle, const char* pkey)
    /// k2h_remove_str_all: Remove all values for a key
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key string
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_remove_str_all(handle: u64, pkey: *const c_char) -> bool;

    /// # bool k2h_remove_str(k2h_h handle, const char* pkey)
    /// k2h_remove_str: Remove values for a key
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key string
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_remove_str(handle: u64, pkey: *const c_char) -> bool;

    /// # rename API
    /// # bool k2h_rename_str(k2h_h handle, const char* pkey, const char* pnewkey)
    /// k2h_rename_str: Rename a key
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - old key string
    /// * `pnewkey` - new key string
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_rename_str(handle: u64, pkey: *const c_char, pnewkey: *const c_char) -> bool;

    /// # remove subkey API
    /// # bool k2h_remove_str_subkey(k2h_h handle, const char* pkey, const char* psubkey)
    /// k2h_remove_str_subkey: Remove a subkey from a key
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key string
    /// * `psubkey` - subkey string
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_remove_str_subkey(handle: u64, pkey: *const c_char, psubkey: *const c_char) -> bool;

    /// # set_common_attr
    /// # bool k2h_set_common_attr(k2h_h handle, const bool* is_mtime, const bool* is_defenc, const char* passfile, const bool* is_history, const c_ulong* expire)
    /// k2h_set_common_attr: Set common attributes
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `is_mtime` - pointer to bool
    /// * `is_defenc` - pointer to bool
    /// * `passfile` - file path string
    /// * `is_history` - pointer to bool
    /// * `expire` - pointer to c_ulong
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_set_common_attr(
        handle: u64,
        is_mtime: *const bool,
        is_defenc: *const bool,
        passfile: *const c_char,
        is_history: *const bool,
        expire: *const c_ulong,
    ) -> bool;

    // # set loglevel
    /// # bool k2h_set_debug_level_silent(void)
    /// k2h_set_debug_level_silent: Set debug level to silent
    ///
    /// # Arguments
    /// * `void` - no arguments
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_set_debug_level_silent() -> bool;

    /// # bool k2h_set_debug_level_error(void)
    /// k2h_set_debug_level_error: Set debug level to error
    ///
    /// # Arguments
    /// * `void` - no arguments
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_set_debug_level_error() -> bool;

    /// # bool k2h_set_debug_level_warning(void)
    /// k2h_set_debug_level_warning: Set debug level to warning
    ///
    /// # Arguments
    /// * `void` - no arguments
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_set_debug_level_warning() -> bool;

    /// # bool k2h_set_debug_level_message(void)
    /// k2h_set_debug_level_message: Set debug level to message
    ///
    /// # Arguments
    /// * `void` - no arguments
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_set_debug_level_message() -> bool;

    /// # set value
    /// # bool k2h_set_str_value_wa(k2h_h handle, const char* pkey, const char* pval, const char* pass, const time_t* expire)
    /// k2h_set_str_value_wa: Set string value with optional password and expiration
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `pkey` - key string
    /// * `pval` - value string
    /// * `pass` - password string (nullable)
    /// * `expire` - pointer to expiration time (nullable)
    ///
    /// # Returns
    /// * `bool` - true on success
    /// fn k2h_set_str_value_wa(
    ///     handle: u64,
    ///     pkey: *const c_char,
    ///     pval: *const c_char,
    ///     pass: *const c_char,
    ///     expire: *const c_ulonglong,
    /// ) -> bool;

    /// # bool k2h_transaction_param(k2h_h handle, bool enable, const char* transfile, const unsigned char* pprefix, size_t prefixlen, const unsigned char* pparam, size_t paramlen)
    /// k2h_transaction_param: Set transaction parameters
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `enable` - enable flag
    /// * `transfile` - transaction file path
    /// * `pprefix` - prefix pointer
    /// * `prefixlen` - prefix length
    /// * `pparam` - parameter pointer
    /// * `paramlen` - parameter length
    ///
    /// # Returns
    /// * `bool` - true on success
    // fn k2h_transaction_param(
    //     handle: u64,
    //     enable: bool,
    //     transfile: *const c_char,
    //     pprefix: *const u8,
    //     prefixlen: usize,
    //     pparam: *const u8,
    //     paramlen: usize,
    // ) -> bool;

    /// # bool k2h_transaction_param_we(k2h_h handle, bool enable, const char* transfile, const unsigned char* pprefix, size_t prefixlen, const unsigned char* pparam, size_t paramlen, const time_t* expire)
    /// k2h_transaction_param_we: Set transaction parameters with expiration
    ///
    /// # Arguments
    /// * `handle` - k2hash handle
    /// * `enable` - enable flag
    /// * `transfile` - transaction file path
    /// * `pprefix` - prefix pointer
    /// * `prefixlen` - prefix length
    /// * `pparam` - parameter pointer
    /// * `paramlen` - parameter length
    /// * `expire` - pointer to expiration time
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_transaction_param_we(
        handle: u64,
        enable: bool,
        transfile: *const c_char,
        pprefix: *const u8,
        prefixlen: usize,
        pparam: *const u8,
        paramlen: usize,
        expire: *const c_ulonglong,
    ) -> bool;

    /// # bool k2h_set_transaction_thread_pool(int count)
    /// k2h_set_transaction_thread_pool: Set transaction thread pool count
    ///
    /// # Arguments
    /// * `count` - thread pool count
    ///
    /// # Returns
    /// * `bool` - true on success
    fn k2h_set_transaction_thread_pool(count: c_int) -> bool;

}

/// K2hashKey provides an iterator over the keys in the K2hash database.
///
/// # Examples
/// https://doc.rust-lang.org/std/iter/index.html#implementing-iterator
/// ```
/// use k2hash_rust::{K2hash, K2hashKey};
/// let db = K2hash::open_mem().expect("open_mem failed");
/// assert!(db.handle() != 0, "Database handle should not be zero");
/// let key = "hello";
/// let val = "world";
/// assert!(db.set(key, val).is_ok(), "Set operation failed");
/// assert_eq!(
///     db.get(key).expect("Get operation failed"),
///     Some("world".to_string()),
///     "Get operation returned unexpected value"
/// );
/// let mut k2hkey = K2hashKey::new(db.handle(), None).expect("K2hashKey creation failed"); // internally calls k2h_find_first.
/// assert_eq!(k2hkey.next(), Some("hello".to_string())); // internally calls k2h_find_next.
/// assert_eq!(k2hkey.next(), None); // internally calls k2h_find_next, but no more keys are available, so returns None.
/// ```
pub struct K2hashKey {
    k2h_handle: u64,
    key: Option<String>,
    handle: u64,
}

impl K2hashKey {
    pub fn new(k2h_handle: u64, key: Option<String>) -> Result<Self, &'static str> {
        if k2h_handle == 0 {
            return Err("handle should not be 0");
        }
        let mut k2hkey = K2hashKey {
            k2h_handle,
            key,
            handle: 0,
        };
        if let Some(ref k) = k2hkey.key {
            // if key is provided, call k2h_find_first_str_subkey.
            let c_key = CString::new(k.as_str()).unwrap();
            k2hkey.handle = unsafe { k2h_find_first_str_subkey(k2hkey.k2h_handle, c_key.as_ptr()) };
        } else {
            k2hkey.handle = unsafe { k2h_find_first(k2hkey.k2h_handle) };
        }
        if k2hkey.handle == 0 {
            return Err("handle should not be K2H_INVALID_HANDLE");
        }
        Ok(k2hkey)
    }
}

// fn k2h_find_get_key(findhandle: u64, ppkey: *mut *mut c_uchar, pkeylength: *mut usize) -> bool;

// fn k2h_keyq_str_read_keyval_wp(
//     keyqhandle: u64,
//     ppkey: *mut *mut c_char,
//     ppval: *mut *mut c_char,
//     pos: c_int,
//     encpass: *const c_char,
// ) -> bool;

impl Iterator for K2hashKey {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let mut ppkey: *mut c_char = ptr::null_mut();
        let mut pkeylength: usize = 0;
        let result = unsafe {
            k2h_find_get_key(
                self.handle,
                &mut ppkey as *mut *mut c_char as *mut *mut c_uchar,
                &mut pkeylength as *mut usize,
            )
        };
        if result && !ppkey.is_null() && pkeylength > 0 {
            let key = unsafe { CStr::from_ptr(ppkey).to_string_lossy().into_owned() };
            self.handle = unsafe { k2h_find_next(self.handle) };
            Some(key)
        } else {
            None
        }
    }
}

/// K2hash struct provides a high-level interface for interacting with the K2hash database.
///
/// # Examples
///
/// ```
/// use k2hash_rust::K2hash;
/// let db = K2hash::open_mem().expect("open_mem failed");
/// assert!(db.handle() != 0, "Database handle should not be zero");
/// assert!(
///     db.set("test_key", "test_value").is_ok(),
///     "Set operation failed"
/// );
/// let value = db.get("test_key").expect("Get operation failed");
/// assert_eq!(
///     value,
///     Some("test_value".to_string()),
///     "Get operation returned unexpected value"
/// );
/// ```
pub struct K2hash {
    handle: u64,
}

impl K2hash {
    /// Get the handle of the K2hash database.
    pub fn handle(&self) -> u64 {
        self.handle
    }

    /// Set the debug level for the K2hash c-library.
    pub fn set_debug_level(level: DebugLevel) -> Result<(), &'static str> {
        let result = match level {
            DebugLevel::SILENT => unsafe { k2h_set_debug_level_silent() },
            DebugLevel::ERROR => unsafe { k2h_set_debug_level_error() },
            DebugLevel::WARNING => unsafe { k2h_set_debug_level_warning() },
            DebugLevel::MESSAGE => unsafe { k2h_set_debug_level_message() },
        };
        if result {
            Ok(())
        } else {
            Err("Failed to set debug level")
        }
    }

    /// Open a key-value database in a file based.
    pub fn open(file: &str) -> Result<Self, &'static str> {
        let f = CString::new(file).unwrap();
        let handle = unsafe { k2h_open(f.as_ptr(), false, false, false, 8, 4, 1024, 512) };
        if handle == 0 {
            Err("k2h_open_mem failed")
        } else {
            Ok(K2hash { handle })
        }
    }

    /// Open a memory-based database.
    pub fn open_mem() -> Result<Self, &'static str> {
        let handle = unsafe { k2h_open_mem(8, 4, 1024, 512) };
        if handle == 0 {
            Err("k2h_open_mem failed")
        } else {
            Ok(K2hash { handle })
        }
    }

    /// Set a value with a key.
    pub fn set(&self, key: &str, value: &str) -> Result<(), &'static str> {
        return self.set_with_options(key, value, None, None);
    }

    /// Set a value with a key with the options.
    pub fn set_with_options(
        &self,
        key: &str,
        value: &str,
        password: Option<&str>,
        expire_duration: Option<u64>,
    ) -> Result<(), &'static str> {
        let k = CString::new(key).unwrap();
        let v = CString::new(value).unwrap();
        if k.is_empty() || v.is_empty() {
            return Err("Key and value cannot be empty");
        }
        // Option<String> --as_ref()--> Option<&String> --CString::new()-> Option<CString>
        let pass = password.map(|p| CString::new(p).unwrap());
        let c_pass = pass.as_ref().map_or(ptr::null(), |p| p.as_ptr());
        let expire = expire_duration.map(|e| e as c_ulonglong);
        let c_expire = expire
            .as_ref()
            .map_or(ptr::null(), |e| e as *const c_ulonglong);
        let result =
            unsafe { k2h_set_str_value_wa(self.handle, k.as_ptr(), v.as_ptr(), c_pass, c_expire) };
        if result {
            Ok(())
        } else {
            Err("Failed to set value")
        }
    }

    /// Get a value from a key.
    pub fn get(&self, key: &str) -> Result<Option<String>, &'static str> {
        return self.get_with_options(key, None);
    }

    /// Get a value from a key with options.
    pub fn get_with_options(
        &self,
        key: &str,
        password: Option<&str>,
    ) -> Result<Option<String>, &'static str> {
        let k = CString::new(key).unwrap();
        if k.is_empty() {
            return Err("Key cannot be empty");
        }
        // Option<&str> --CString::new()-> Option<CString>
        let pass = password.map(|p| CString::new(p).unwrap());
        let c_pass = pass.as_ref().map_or(ptr::null(), |p| p.as_ptr());
        let ptr = unsafe { k2h_get_str_direct_value_wp(self.handle, k.as_ptr(), c_pass) };

        if ptr.is_null() {
            Err("Failed to get result")
        } else {
            let cstr = unsafe { CStr::from_ptr(ptr) };
            Ok(Some(cstr.to_string_lossy().into_owned()))
        }
    }

    /// Add an attribute plugin library.
    pub fn add_attribute_plugin_lib(&self, path: &str) -> Result<(), &'static str> {
        let path = CString::new(path).unwrap();
        if path.is_empty() {
            return Err("Path should be a some string");
        }
        let result = unsafe { k2h_add_attr_plugin_library(self.handle, path.as_ptr()) };
        if result {
            Ok(())
        } else {
            Err("add_attribute_plugin_lib returns error")
        }
    }

    /// Add a decryption password.
    pub fn add_decryption_password(&self, password: &str) -> Result<(), &'static str> {
        let password = CString::new(password).unwrap();
        if password.is_empty() {
            return Err("Password should be a some string");
        }
        let result = unsafe { k2h_add_attr_crypt_pass(self.handle, password.as_ptr(), false) };
        if result {
            Ok(())
        } else {
            Err("add_decryption_password returns error")
        }
    }

    /// Add a subkey to a key.
    pub fn add_subkey(&self, key: &str, subkey: &str, subval: &str) -> Result<(), &'static str> {
        return self.add_subkey_with_options(key, subkey, subval, None, None);
    }

    /// Add a subkey to a key with options.
    pub fn add_subkey_with_options(
        &self,
        key: &str,
        subkey: &str,
        subval: &str,
        password: Option<String>,
        expire_duration: Option<u64>,
    ) -> Result<(), &'static str> {
        let k = CString::new(key).unwrap();
        let sk = CString::new(subkey).unwrap();
        let sv = CString::new(subval).unwrap();
        // Option<String> --as_ref()--> Option<&String> --CString::new()-> Option<CString>
        let pass = password.as_ref().map(|p| CString::new(p.as_str()).unwrap());
        let c_pass = pass.as_ref().map_or(ptr::null(), |p| p.as_ptr());
        let expire = expire_duration.map(|e| e as c_ulonglong);
        let c_expire = expire
            .as_ref()
            .map_or(ptr::null(), |e| e as *const c_ulonglong);
        let result = unsafe {
            k2h_add_subkey_wa(
                self.handle,
                k.as_bytes_with_nul().as_ptr(),
                k.as_bytes_with_nul().len(),
                sk.as_bytes_with_nul().as_ptr(),
                sk.as_bytes_with_nul().len(),
                sv.as_bytes_with_nul().as_ptr(),
                sv.as_bytes_with_nul().len(),
                c_pass,
                c_expire,
            )
        };
        if result {
            Ok(())
        } else {
            Err("k2h_add_subkey_wa returns error")
        }
    }

    /// Start a transaction.
    pub fn begin_tx(&self, txfile: &str) -> Result<(), &'static str> {
        return self.begin_tx_with_options(txfile, None, None, None);
    }

    /// Start a transaction with options.
    pub fn begin_tx_with_options(
        &self,
        txfile: &str,
        prefix: Option<String>,
        param: Option<String>,
        expire_duration: Option<u64>,
    ) -> Result<(), &'static str> {
        let txf = CString::new(txfile).unwrap();

        // Option<String> --as_ref()--> Option<&String> --CString::new()-> Option<CString>
        let pre = prefix.as_ref().map(|p| CString::new(p.as_str()).unwrap());
        // Option<CString> -> const unsigned char*
        let c_pre = pre
            .as_ref()
            .map_or(ptr::null(), |p| p.as_bytes_with_nul().as_ptr());
        // Option<usize>
        let pre_length = pre.as_ref().map(|c| c.as_bytes_with_nul().len());

        // Option<String> --as_ref()--> Option<&String> --CString::new()-> Option<CString>
        let par = param.as_ref().map(|p| CString::new(p.as_str()).unwrap());
        // Option<CString> -> const unsigned char*
        let c_par = par
            .as_ref()
            .map_or(ptr::null(), |p| p.as_bytes_with_nul().as_ptr());
        // Option<usize>
        let par_length = par.as_ref().map(|c| c.as_bytes_with_nul().len());

        let expire = expire_duration.map(|e| e as c_ulonglong);
        let c_expire = expire
            .as_ref()
            .map_or(ptr::null(), |e| e as *const c_ulonglong);
        let result = unsafe {
            k2h_transaction_param_we(
                self.handle,
                true,
                txf.as_ptr(),
                c_pre,
                pre_length.unwrap_or(0), // Option<usize> -> usize
                c_par,
                par_length.unwrap_or(0), // Option<usize> -> usize
                c_expire,
            )
        };
        if result {
            Ok(())
        } else {
            Err("begin_tx returns error")
        }
    }

    /// Create a new K2hash database in a file.
    pub fn create(path: &str) -> Result<(), &'static str> {
        return K2hash::create_with_options(path, 8, 4, 1024, 512);
    }

    /// Create a new K2hash database in a file with options.
    pub fn create_with_options(
        path: &str,
        maskbit: i32,
        cmaskbit: i32,
        maxelementcnt: i32,
        pagesize: usize,
    ) -> Result<(), &'static str> {
        let p = CString::new(path).unwrap();
        if p.is_empty() {
            return Err("path cannot be empty");
        }
        let maskbit_len: i32 = maskbit;
        let cmaskbit_len: i32 = cmaskbit;
        let maxelementcnt_len: i32 = maxelementcnt;
        let pagesize_len: usize = pagesize;

        let result = unsafe {
            k2h_create(
                p.as_ptr(),
                maskbit_len,
                cmaskbit_len,
                maxelementcnt_len,
                pagesize_len,
            )
        };
        if result {
            Ok(())
        } else {
            Err("k2h_create returns error")
        }
    }

    /// Dump the database to a file.
    pub fn dump_to_file(&self, path: &str) -> Result<(), &'static str> {
        return self.dump_to_file_with_options(path, true);
    }

    /// Dump the database to a file with options.
    pub fn dump_to_file_with_options(
        &self,
        path: &str,
        is_skip_error: bool,
    ) -> Result<(), &'static str> {
        let p = CString::new(path).unwrap();
        if p.is_empty() {
            return Err("path cannot be empty");
        }
        let i: bool = is_skip_error;

        let result = unsafe { k2h_put_archive(self.handle, p.as_ptr(), i) };
        if result {
            Ok(())
        } else {
            Err("k2h_create returns error")
        }
    }

    /// Enable encryption.
    pub fn enable_encryption(&self, enable: bool) -> Result<(), &'static str> {
        let is_defenc = enable;
        let result = unsafe {
            k2h_set_common_attr(
                self.handle,
                ptr::null(),               // is_mtime
                &is_defenc as *const bool, // is_defenc
                ptr::null(),               // passfile
                ptr::null(),               // is_history
                ptr::null(),               // expire
            )
        };
        if result {
            Ok(())
        } else {
            Err("k2h_create returns error")
        }
    }

    /// Enable history.
    pub fn enable_history(&self, enable: bool) -> Result<(), &'static str> {
        let is_history = enable;
        let result = unsafe {
            k2h_set_common_attr(
                self.handle,
                ptr::null(),                // is_mtime
                ptr::null(),                // is_defenc
                ptr::null(),                // passfile
                &is_history as *const bool, // is_history
                ptr::null(),                // expire
            )
        };
        if result {
            Ok(())
        } else {
            Err("k2h_create returns error")
        }
    }

    /// Enable mtime.
    pub fn enable_mtime(&self, enable: bool) -> Result<(), &'static str> {
        let is_mtime = enable;
        let result = unsafe {
            k2h_set_common_attr(
                self.handle,
                &is_mtime as *const bool, // is_mtime
                ptr::null(),              // is_defenc
                ptr::null(),              // passfile
                ptr::null(),              // is_history
                ptr::null(),              // expire
            )
        };
        if result {
            Ok(())
        } else {
            Err("k2h_create returns error")
        }
    }

    /// Get attributes of a key.
    pub fn get_attributes(
        &self,
        key: &str,
    ) -> Result<Option<HashMap<String, String>>, &'static str> {
        let k = CString::new(key).unwrap();
        if k.is_empty() {
            return Err("key should be passed");
        }
        let k_length = k.as_bytes_with_nul().len();
        let mut pattrspckcnt: c_int = 0; // mutable c_int
                                         // let mut pskeypckcnt: c_int = 0;
        let result = unsafe {
            k2h_get_direct_attrs(
                self.handle,
                k.as_bytes_with_nul().as_ptr(), // byte
                k_length,
                &mut pattrspckcnt, // mutable pointer to c_int
            )
        };
        println!("pattrspckcnt: {}", pattrspckcnt);
        if !result.is_null() && pattrspckcnt > 0 {
            let mut attrs: HashMap<String, String> = HashMap::new();
            for i in 0..pattrspckcnt {
                // Assuming result is a pointer to an array of AttrPack structures
                let attr_pack: &K2hAttrPack =
                    unsafe { &*(result as *const K2hAttrPack).add(i as usize) };
                let key = unsafe {
                    CStr::from_ptr(attr_pack.pkey as *const c_char)
                        .to_string_lossy()
                        .into_owned()
                };
                let val = unsafe {
                    CStr::from_ptr(attr_pack.pval as *const c_char)
                        .to_string_lossy()
                        .into_owned()
                };
                // println!("key: {}", key);
                let keylength = unsafe { attr_pack.keylength as usize };
                let vallength = unsafe { attr_pack.vallength as usize };
                println!("keylength: {}", keylength);
                println!("vallength: {}", vallength);
                attrs.insert(key, val);
            }
            // Return the attributes as a Result
            Ok(Some(attrs))
        } else {
            Err("k2h_get_direct_attrs returns error")
        }
    }

    // # get subkeys API
    // # PK2HKEYPCK k2h_get_direct_subkeys(k2h_h handle, const unsigned char* pkey,
    // # size_t keylength, int* pskeypckcnt)
    // ret.k2h_get_direct_subkeys.argtypes = [c_uint64, c_char_p, c_size_t, POINTER(c_int)]
    // ret.k2h_get_direct_subkeys.restype = POINTER(KeyPack)

    /// Get subkeys of a key.
    pub fn get_subkeys(&self, key: &str) -> Result<Option<Vec<String>>, &'static str> {
        let k = CString::new(key).unwrap();
        if k.is_empty() {
            return Err("key should be passed");
        }
        let k_length = k.as_bytes_with_nul().len();
        let mut pskeypckcnt: c_int = 0;
        let result = unsafe {
            k2h_get_direct_subkeys(
                self.handle,
                k.as_bytes_with_nul().as_ptr(),
                k_length,
                &mut pskeypckcnt,
            )
        };
        if !result.is_null() && pskeypckcnt > 0 {
            // println!("pskeypckcnt: {}", pskeypckcnt);
            let mut keys: Vec<String> = Vec::new();
            for i in 0..pskeypckcnt {
                // Assuming result is a pointer to an array of KeyPack structures
                let key_pack: &K2hKeyPack =
                    unsafe { &*(result as *const K2hKeyPack).add(i as usize) };
                let key = unsafe {
                    CStr::from_ptr(key_pack.pkey as *const c_char)
                        .to_string_lossy()
                        .into_owned()
                };
                // println!("key: {}", key);
                keys.push(key);
            }
            // Return the keys as a Result
            Ok(Some(keys))
        } else {
            Err("k2h_get_direct_subkeys returns error")
        }
    }

    /// Get transaction file descriptor.
    pub fn get_tx_file_fd(&self) -> Result<i32, &'static str> {
        // fn k2h_get_transaction_archive_fd(handle: u64) -> c_int;
        let fd = unsafe { k2h_get_transaction_archive_fd(self.handle) };
        if fd < 0 {
            Err("Failed to get result")
        } else {
            Ok(fd)
        }
    }

    /// Get transaction pool size.
    pub fn get_tx_pool_size() -> Result<i32, &'static str> {
        let pool_size = unsafe { k2h_get_transaction_thread_pool() };
        if pool_size < 0 {
            Err("Failed to get result")
        } else {
            Ok(pool_size)
        }
    }

    /// Load a K2hash database from a file.
    pub fn load_from_file(
        &self,
        path: &str,
        is_skip_error: Option<bool>,
    ) -> Result<(), &'static str> {
        let p = CString::new(path).unwrap();
        let skip_error = is_skip_error.unwrap_or(true);
        if p.is_empty() {
            return Err("Failed to get path");
        }
        let result = unsafe { k2h_load_archive(self.handle, p.as_ptr(), skip_error) };
        if result {
            Ok(())
        } else {
            Err("k2h_load_archive returns error")
        }
    }

    /// Print attribute plugins of the K2hash database.
    pub fn print_attribute_plugins(&self) -> Result<(), &'static str> {
        let result = unsafe { k2h_print_attr_version(self.handle, core::ptr::null_mut()) };
        if result {
            Ok(())
        } else {
            Err("k2h_print_attr_version returns error")
        }
    }

    /// Print attribute information of k2hash database.
    pub fn print_attributes(&self) -> Result<(), &'static str> {
        let result = unsafe { k2h_print_attr_information(self.handle, core::ptr::null_mut()) };
        if result {
            Ok(())
        } else {
            Err("k2h_print_attr_information returns error")
        }
    }

    /// Print attribute information of k2hash database.
    pub fn print_data_stats(&self) -> Result<(), &'static str> {
        let result = unsafe { k2h_print_state(self.handle, core::ptr::null_mut()) };
        if result {
            Ok(())
        } else {
            Err("k2h_print_state returns error")
        }
    }

    /// Print table information of k2hash database.
    pub fn print_table_stats(&self, dump_level: DumpLevel) -> Result<(), &'static str> {
        let dl = dump_level;
        if dl == DumpLevel::HEADER {
            let result = unsafe { k2h_dump_head(self.handle, core::ptr::null_mut()) };
            if result {
                Ok(())
            } else {
                Err("k2h_dump_head returns error")
            }
        } else if dl == DumpLevel::HASHTABLE {
            let result = unsafe { k2h_dump_keytable(self.handle, core::ptr::null_mut()) };
            if result {
                Ok(())
            } else {
                Err("k2h_dump_head returns error")
            }
        } else if dl == DumpLevel::SUBHASHTABLE {
            let result = unsafe { k2h_dump_full_keytable(self.handle, core::ptr::null_mut()) };
            if result {
                Ok(())
            } else {
                Err("k2h_dump_head returns error")
            }
        } else if dl == DumpLevel::ELEMENT {
            let result = unsafe { k2h_dump_elementtable(self.handle, core::ptr::null_mut()) };
            if result {
                Ok(())
            } else {
                Err("k2h_dump_head returns error")
            }
        } else if dl == DumpLevel::PAGE {
            let result = unsafe { k2h_dump_full(self.handle, core::ptr::null_mut()) };
            if result {
                Ok(())
            } else {
                Err("k2h_dump_head returns error")
            }
        } else {
            return Err("k2h_dump_head returns error");
        }
    }

    /// Remove a key from the K2hash database.
    pub fn remove(&self, key: &str) -> Result<(), &'static str> {
        return self.remove_with_options(key, false);
    }

    /// Remove a key from the K2hash database with options.
    pub fn remove_with_options(
        &self,
        key: &str,
        remove_all_subkeys: bool,
    ) -> Result<(), &'static str> {
        let k = CString::new(key).unwrap();
        let b_remove_all_subkeys = remove_all_subkeys;
        if k.is_empty() {
            return Err("key should be passed");
        }
        if b_remove_all_subkeys {
            let result = unsafe { k2h_remove_str_all(self.handle, k.as_ptr()) };
            if result {
                Ok(())
            } else {
                Err("Failed to h.k2h_remove_str_all")
            }
        } else {
            let result = unsafe { k2h_remove_str(self.handle, k.as_ptr()) };
            if result {
                Ok(())
            } else {
                Err("Failed to set value")
            }
        }
    }

    /// Remove a subkey of a key from the K2hash database.
    pub fn remove_subkeys(&self, key: &str, subkeys: Vec<&str>) -> Result<(), &'static str> {
        let k = CString::new(key).unwrap();
        if k.is_empty() {
            return Err("key should be passed");
        }
        let v_subkeys_iter = subkeys.iter();
        for skey in v_subkeys_iter {
            let sk = CString::new(*skey).unwrap();
            let result = unsafe { k2h_remove_str_subkey(self.handle, k.as_ptr(), sk.as_ptr()) };
            if result == false {
                return Err("Failed to h.k2h_remove_str_all");
            }
        }
        Ok(())
    }

    /// Rename a old key with a new key.
    pub fn rename(&self, oldkey: &str, newkey: &str) -> Result<(), &'static str> {
        let okey = CString::new(oldkey).unwrap();
        if okey.is_empty() {
            return Err("oldkey should be passed");
        }
        let nkey = CString::new(newkey).unwrap();
        if nkey.is_empty() {
            return Err("newkey should be passed");
        }
        let result = unsafe { k2h_rename_str(self.handle, okey.as_ptr(), nkey.as_ptr()) };
        if result {
            Ok(())
        } else {
            Err("Failed to h.k2h_rename")
        }
    }

    /// Set attribute of a key in the K2hash database.
    pub fn set_attribute(
        &self,
        key: &str,
        attr_name: &str,
        attr_val: &str,
    ) -> Result<(), &'static str> {
        let key = CString::new(key).unwrap();
        if key.as_bytes().is_empty() {
            return Err("key should be passed");
        }
        let k_length = key.as_bytes_with_nul().len();

        let attr_name = CString::new(attr_name).unwrap();
        if attr_name.as_bytes().is_empty() {
            return Err("attr_name should be passed");
        }
        let name_length = attr_name.as_bytes_with_nul().len();

        let attr_val = CString::new(attr_val).unwrap();
        if attr_val.as_bytes().is_empty() {
            return Err("attr_val should be passed");
        }
        let val_length = attr_val.as_bytes_with_nul().len();

        let result = unsafe {
            k2h_add_attr(
                self.handle,
                key.as_bytes_with_nul().as_ptr(),
                k_length,
                attr_name.as_bytes_with_nul().as_ptr(),
                name_length,
                attr_val.as_bytes_with_nul().as_ptr(),
                val_length,
            )
        };
        if result {
            Ok(())
        } else {
            Err("Failed to h.k2h_set_attribute")
        }
    }

    /// Set a default encryption password.
    pub fn set_default_encryption_password(&self, password: &str) -> Result<(), &'static str> {
        let p = CString::new(password).unwrap();
        if p.is_empty() {
            return Err("password should be passed");
        }
        let result = unsafe { k2h_add_attr_crypt_pass(self.handle, p.as_ptr(), true) };
        if result {
            Ok(())
        } else {
            Err("Failed to set value")
        }
    }

    /// Set subkeys to a key in the K2hash database.
    pub fn set_subkeys(&self, key: &str, subkeys: Vec<(&str, &str)>) -> Result<(), &'static str> {
        return self.set_subkeys_with_options(key, subkeys, None, None);
    }

    /// Set subkeys to a key in the K2hash database with options.
    pub fn set_subkeys_with_options(
        &self,
        key: &str,
        subkeys: Vec<(&str, &str)>,
        password: Option<String>,
        expire_duration: Option<u64>,
    ) -> Result<(), &'static str> {
        let k = CString::new(key).unwrap();
        if k.is_empty() {
            return Err("key should be passed");
        }
        let k_length = k.as_bytes_with_nul().len();
        // Option<String> --as_ref()--> Option<&String> --CString::new()-> Option<CString>
        let pass = password.as_ref().map(|p| CString::new(p.as_str()).unwrap());
        // Option<CString> -> const unsigned char*
        let c_pass = pass.as_ref().map_or(ptr::null(), |p| p.as_ptr());
        // Option<usize>
        let expire = expire_duration.map(|e| e as c_ulonglong);
        let c_expire = expire
            .as_ref()
            .map_or(ptr::null(), |e| e as *const c_ulonglong);

        let v_subkeys_iter = subkeys.iter();
        for (subk, subv) in v_subkeys_iter {
            let sk = CString::new(*subk).unwrap();
            let sv = CString::new(*subv).unwrap();
            if sk.as_bytes().is_empty() {
                return Err("subkey's key should be passed");
            }
            let sk_length = sk.as_bytes_with_nul().len();
            if sv.as_bytes().is_empty() {
                return Err("subkey's val should be passed");
            }
            let sv_length = sv.as_bytes_with_nul().len();
            let result = unsafe {
                k2h_add_subkey_wa(
                    self.handle,
                    k.as_bytes_with_nul().as_ptr(),
                    k_length,
                    sk.as_bytes_with_nul().as_ptr(),
                    sk_length,
                    sv.as_bytes_with_nul().as_ptr(),
                    sv_length,
                    c_pass,
                    c_expire,
                )
            };
            if result == false {
                return Err("Failed to h.k2h_remove_str_all");
            }
        }
        Ok(())
    }

    /// Stop a transaction.
    pub fn stop_tx(&self) -> Result<(), &'static str> {
        let result = unsafe { k2h_disable_transaction(self.handle) };
        if result {
            Ok(())
        } else {
            Err("k2h_disable_transaction returns error")
        }
    }

    /// Print the k2hash C-library version.
    pub fn version() -> Result<(), &'static str> {
        unsafe { k2h_print_version(core::ptr::null_mut()) };
        Ok(())
    }

    /// Set transaction pool size.
    pub fn set_tx_pool_size(size: i32) -> Result<(), &'static str> {
        if size < 0 {
            return Err("size should be 0 or positive");
        }
        let result = unsafe { k2h_set_transaction_thread_pool(size) };
        if result {
            Ok(())
        } else {
            Err("Failed to set value")
        }
    }
}

impl Drop for K2hash {
    fn drop(&mut self) {
        unsafe {
            k2h_close(self.handle);
        }
    }
}

/// Base struct of Queue and KeyQueue struct.
pub struct BaseQueue {
    k2h: u64,
    fifo: bool,
    prefix: Option<String>,
    password: Option<String>,
    expire_duration: Option<u64>,
    handle: u64,
}
impl BaseQueue {
    /// Create a new BaseQueue.
    pub fn new(
        k2h: u64,
        fifo: bool,
        prefix: Option<String>,
        password: Option<String>,
        expire_duration: Option<u64>,
    ) -> Self {
        BaseQueue {
            k2h,
            fifo,
            prefix,
            password,
            expire_duration,
            handle: 0,
        }
    }

    /// Get the handle of the BaseQueue.
    pub fn handle(&self) -> u64 {
        self.handle
    }
}

/// Queue provides FIFO (first-in, first-out) functionality using k2hash database.
///
/// # Examples
///
/// ```
/// use k2hash_rust::{K2hash, Queue};
/// let db = K2hash::open_mem().expect("open_mem failed");
/// let handle = db.handle();
/// assert!(handle != 0, "Handle should not be zero");
/// let q = Queue::new(db.handle(), true, None, None, None).expect("Queue creation failed");
/// // check if queue is not null
/// assert!(q.handle() != 0, "Queue handle should not be zero");
/// // converts &str(string slice) to String
/// let value = "hello".to_string();
/// assert!(q.put(&value).is_ok(), "Push operation failed");
/// // check if get returns the correct value
/// if let Some(value) = q.get() {
///     assert_eq!(value, "hello", "Get operation returned unexpected value");
/// } else {
///     panic!("Get operation failed or returned None");
/// }
/// ```
pub struct Queue {
    base: BaseQueue,
}
/// KeyQueue provides FIFO (first-in, first-out) functionality using k2hash database.
///
/// # Examples
///
/// ```
/// use k2hash_rust::{K2hash, KeyQueue};
/// let db = K2hash::open_mem().expect("open_mem failed");
/// let handle = db.handle();
/// assert!(handle != 0, "Handle should not be zero");
/// let q = KeyQueue::new(db.handle(), true, None, None, None).expect("KeyQueue creation failed");
/// // check if KeyQueue is not null
/// assert!(q.handle() != 0, "KeyQueue handle should not be zero");
/// // converts &str(string slice) to String
/// let key = "hello";
/// let value = "world";
/// assert!(q.put(key, value).is_ok(), "Push operation failed");
/// // check if get returns the correct value
/// if let Some((key, value)) = q.get() {
///     assert_eq!(key, "hello", "Get operation returned unexpected value");
///     assert_eq!(value, "world", "Get operation returned unexpected value");
/// } else {
///     panic!("Get operation failed or returned None");
/// }
/// ```
pub struct KeyQueue {
    base: BaseQueue,
}
impl Queue {
    /// Create a new Queue.
    pub fn new(
        k2h: u64,
        fifo: bool,
        prefix: Option<String>,
        password: Option<String>,
        expire_duration: Option<u64>,
    ) -> Result<Self, &'static str> {
        let mut base = BaseQueue::new(k2h, fifo, prefix, password, expire_duration);
        let c_prefix = base
            .prefix
            .as_ref()
            .map(|p| CString::new(p.as_str()).unwrap());
        let ptr = c_prefix.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        base.handle = unsafe { k2h_q_handle_str_prefix(base.k2h, base.fifo, ptr) };
        if base.handle == 0 {
            Err("Queue instance failed")
        } else {
            Ok(Queue { base })
        }
    }

    /// Get a handle to the Queue.
    pub fn handle(&self) -> u64 {
        self.base.handle
    }

    /// Put a value into the Queue.
    pub fn put(&self, value: &str) -> Result<(), &'static str> {
        let c_val = CString::new(value).unwrap();
        let c_pass = self
            .base
            .password
            .as_ref()
            .map(|p| CString::new(p.as_str()).unwrap());
        let c_pattrspck = ptr::null();
        let c_attrspckcnt = 0;
        let expire = self.base.expire_duration.map(|e| e as c_ulonglong);
        let result = unsafe {
            k2h_q_str_push_wa(
                self.base.handle,
                c_val.as_ptr(),
                c_pattrspck,
                c_attrspckcnt,
                c_pass.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
                expire
                    .as_ref()
                    .map_or(ptr::null(), |e| e as *const c_ulonglong),
            )
        };
        if result {
            Ok(())
        } else {
            Err("Failed to set value")
        }
    }

    /// Get a value from the Queue.
    pub fn get(&self) -> Option<String> {
        let mut val_ptr: *mut c_char = ptr::null_mut();
        let c_pass = self
            .base
            .password
            .as_ref()
            .map(|p| CString::new(p.as_str()).unwrap());
        let result = unsafe {
            k2h_q_str_pop_wp(
                self.base.handle,
                &mut val_ptr,
                c_pass.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
            )
        };
        if result && !val_ptr.is_null() {
            let cstr = unsafe { CStr::from_ptr(val_ptr) };
            Some(cstr.to_string_lossy().into_owned())
        } else {
            None
        }
    }

    /// Get the size of the Queue.
    pub fn qsize(&self) -> usize {
        unsafe { k2h_q_count(self.base.handle) as usize }
    }

    /// Get an element from the Queue at a specific position.
    pub fn element(&self, position: usize) -> Option<String> {
        let mut ppdata: *mut c_char = ptr::null_mut();
        let c_pass = self
            .base
            .password
            .as_ref()
            .map(|p| CString::new(p.as_str()).unwrap());
        let result = unsafe {
            k2h_q_str_read_wp(
                self.base.handle,
                &mut ppdata,
                position as c_int,
                c_pass.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
            )
        };
        if result && !ppdata.is_null() {
            let cstr = unsafe { CStr::from_ptr(ppdata) };
            Some(cstr.to_string_lossy().into_owned())
        } else {
            None
        }
    }

    /// Check if the Queue is empty.
    pub fn empty(&self) -> bool {
        unsafe { k2h_q_empty(self.base.handle) }
    }

    /// Print the objects in the Queue.
    pub fn print(&self) -> Result<(), &'static str> {
        let result = unsafe { k2h_q_dump(self.base.handle, ptr::null_mut()) };
        if result {
            Ok(())
        } else {
            Err("k2h_q_dump returns error")
        }
    }

    /// Remove the objects from the Queue.
    pub fn remove(&self, count: usize) -> Result<Vec<String>, &'static str> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut vals = Vec::new();
        for _ in 0..count {
            let mut val_ptr: *mut c_char = ptr::null_mut();
            let c_pass = self
                .base
                .password
                .as_ref()
                .map(|p| CString::new(p.as_str()).unwrap());
            let result = unsafe {
                k2h_q_str_pop_wp(
                    self.base.handle,
                    &mut val_ptr,
                    c_pass.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
                )
            };
            if result && !val_ptr.is_null() {
                let cstr = unsafe { CStr::from_ptr(val_ptr) };
                vals.push(cstr.to_string_lossy().into_owned());
            } else {
                break; // Stop if no more elements are available
            }
        }
        Ok(vals)
    }

    /// Remove the objects from the Queue.
    pub fn clear(&self) -> bool {
        let count = self.qsize() as c_int;
        if count > 0 {
            unsafe { k2h_q_remove(self.base.handle, count) }
        } else {
            true
        }
    }

    /// Close the Queue.
    pub fn close(&self) -> bool {
        unsafe { k2h_q_free(self.base.handle) }
    }
}

/// QueueBuilder provides a builder pattern for creating Queue instances.
///
/// # Examples
///
/// ```
/// use k2hash_rust::{K2hash, QueueBuilder};
/// let db = K2hash::open_mem().expect("open_mem failed");
/// let handle = db.handle();
/// assert!(handle != 0, "Handle should not be zero");
/// let fifo = true; // or false, depending on your needs
/// let prefix = "test_prefix".to_string();
/// let password = Some("your_password".to_string());
/// let expire_duration = Some(60); // for 60 seconds expiration
///                                 // Create the queue using QueueBuilder
/// let qb1 = QueueBuilder::new(db.handle())
///     .fifo(fifo)
///     .prefix(prefix) // Optional prefix
///     .password(password.expect("Error")) // Optional password
///     .expire_duration(expire_duration.expect("Error")) // Optional expiration duration
///     .build()
///     .expect("Queue creation failed");
/// // check if queue is not null
/// assert!(qb1.handle() != 0, "Queue handle should not be zero");
/// ```
pub struct QueueBuilder {
    k2h: u64,
    fifo: bool,
    prefix: Option<String>,
    password: Option<String>,
    expire_duration: Option<u64>,
}

impl QueueBuilder {
    /// Create a new QueueBuilder instance.
    pub fn new(k2h: u64) -> Self {
        QueueBuilder {
            k2h,
            fifo: true,
            prefix: None,
            password: None,
            expire_duration: None,
        }
    }

    /// Set the FIFO flag for the Queue.
    pub fn fifo(mut self, fifo: bool) -> Self {
        self.fifo = fifo;
        self
    }
    /// Set the prefix of the Queue.
    pub fn prefix(mut self, prefix: String) -> Self {
        self.prefix = Some(prefix);
        self
    }
    /// Set the password of the Queue.
    pub fn password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }
    /// Set the expiration duration of the Queue.
    pub fn expire_duration(mut self, expire_duration: u64) -> Self {
        self.expire_duration = Some(expire_duration);
        self
    }
    /// Build the Queue.
    pub fn build(self) -> Result<Queue, &'static str> {
        Queue::new(
            self.k2h,
            self.fifo,
            self.prefix,
            self.password,
            self.expire_duration,
        )
    }
}

impl KeyQueue {
    /// Create a new KeyQueue instance.
    pub fn new(
        k2h: u64,
        fifo: bool,
        prefix: Option<String>,
        password: Option<String>,
        expire_duration: Option<u64>,
    ) -> Result<Self, &'static str> {
        let mut base = BaseQueue::new(k2h, fifo, prefix, password, expire_duration);
        let c_prefix = base
            .prefix
            .as_ref()
            .map(|p| CString::new(p.as_str()).unwrap());
        let ptr = c_prefix.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        base.handle = unsafe { k2h_keyq_handle_str_prefix(base.k2h, base.fifo, ptr) };
        if base.handle == 0 {
            Err("Failed to create KeyQueue handle")
        } else {
            Ok(KeyQueue { base })
        }
    }

    /// Get the handle of the KeyQueue.
    pub fn handle(&self) -> u64 {
        self.base.handle
    }

    /// Put a key-value pair into the KeyQueue.
    pub fn put(&self, key: &str, value: &str) -> Result<(), &'static str> {
        let c_key = CString::new(key).unwrap();
        let c_val = CString::new(value).unwrap();
        let c_pass = self
            .base
            .password
            .as_ref()
            .map(|p| CString::new(p.as_str()).unwrap());
        let expire = self.base.expire_duration.map(|e| e as c_ulonglong);
        let result = unsafe {
            k2h_keyq_str_push_keyval_wa(
                self.base.handle,
                c_key.as_ptr(),
                c_val.as_ptr(),
                c_pass.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
                expire
                    .as_ref()
                    .map_or(ptr::null(), |e| e as *const c_ulonglong),
            )
        };
        if result {
            Ok(())
        } else {
            Err("Failed to set value")
        }
    }

    /// Get a key-value pair from the KeyQueue.
    pub fn get(&self) -> Option<(String, String)> {
        let mut key_ptr: *mut c_char = ptr::null_mut();
        let mut val_ptr: *mut c_char = ptr::null_mut();
        let c_pass = self
            .base
            .password
            .as_ref()
            .map(|p| CString::new(p.as_str()).unwrap());
        let result = unsafe {
            k2h_keyq_str_pop_keyval_wp(
                self.base.handle,
                &mut key_ptr,
                &mut val_ptr,
                c_pass.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
            )
        };
        if result && !key_ptr.is_null() && !val_ptr.is_null() {
            let key = unsafe { CStr::from_ptr(key_ptr).to_string_lossy().into_owned() };
            let val = unsafe { CStr::from_ptr(val_ptr).to_string_lossy().into_owned() };
            Some((key, val))
        } else {
            None
        }
    }

    /// Get the size of the KeyQueue.
    pub fn qsize(&self) -> usize {
        unsafe { k2h_keyq_count(self.base.handle) as usize }
    }

    /// Get element from queue in read-only access.
    pub fn element(&self, position: usize) -> Option<(String, String)> {
        if position < 0 {
            return None; // Position should be non-negative
        }
        let mut ppkey: *mut c_char = ptr::null_mut();
        let mut ppval: *mut c_char = ptr::null_mut();
        let c_pass = self
            .base
            .password
            .as_ref()
            .map(|p| CString::new(p.as_str()).unwrap());
        let result = unsafe {
            k2h_keyq_str_read_keyval_wp(
                self.base.handle,
                &mut ppkey,
                &mut ppval,
                position as c_int,
                c_pass.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
            )
        };
        if result && !ppkey.is_null() && !ppval.is_null() {
            let key = unsafe { CStr::from_ptr(ppkey).to_string_lossy().into_owned() };
            let val = unsafe { CStr::from_ptr(ppval).to_string_lossy().into_owned() };
            Some((key, val))
        } else {
            None
        }
    }

    /// Check if the KeyQueue is empty.
    pub fn empty(&self) -> bool {
        unsafe { k2h_keyq_empty(self.base.handle) }
    }

    /// Print the current KeyQueue.
    pub fn print(&self) -> Result<(), &'static str> {
        let result = unsafe { k2h_keyq_dump(self.base.handle, ptr::null_mut()) };
        if result {
            Ok(())
        } else {
            Err("k2h_keyq_dump returns error")
        }
    }

    /// Remove elements from the KeyQueue.
    pub fn remove(&self, count: usize) -> Result<Vec<(String, String)>, &'static str> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut vals = Vec::new();
        for _ in 0..count {
            let mut key_ptr: *mut c_char = ptr::null_mut();
            let mut val_ptr: *mut c_char = ptr::null_mut();
            let c_pass = self
                .base
                .password
                .as_ref()
                .map(|p| CString::new(p.as_str()).unwrap());
            let result = unsafe {
                k2h_keyq_str_pop_keyval_wp(
                    self.base.handle,
                    &mut key_ptr,
                    &mut val_ptr,
                    c_pass.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
                )
            };
            if result && !key_ptr.is_null() && !val_ptr.is_null() {
                let key = unsafe { CStr::from_ptr(key_ptr).to_string_lossy().into_owned() };
                let val = unsafe { CStr::from_ptr(val_ptr).to_string_lossy().into_owned() };
                vals.push((key, val));
            } else {
                break; // Stop if no more elements are available
            }
        }
        Ok(vals)
    }

    /// Remove all elements from the KeyQueue.
    pub fn clear(&self) -> bool {
        let count = self.qsize() as c_int;
        if count > 0 {
            unsafe { k2h_keyq_remove(self.base.handle, count) }
        } else {
            true
        }
    }

    /// Close the KeyQueue handle.
    pub fn close(&self) -> bool {
        unsafe { k2h_keyq_free(self.base.handle) }
    }
}
/// KeyQueueBuilder provides a builder pattern for creating KeyQueue instances.
///
/// # Examples
///
/// ```
/// use k2hash_rust::{K2hash, KeyQueueBuilder};
/// let db = K2hash::open_mem().expect("open_mem failed");
/// let handle = db.handle();
/// assert!(handle != 0, "Handle should not be zero");
/// let fifo = true; // or false, depending on your needs
/// let prefix = "test_prefix".to_string();
/// let password = Some("your_password".to_string());
/// let expire_duration = Some(60); // for 60 seconds expiration
///                                 // Create the KeyQueue using KeyQueueBuilder
/// let qb1 = KeyQueueBuilder::new(db.handle())
///     .fifo(fifo)
///     .prefix(prefix) // Optional prefix
///     .password(password.expect("Error")) // Optional password
///     .expire_duration(expire_duration.expect("Error")) // Optional expiration duration
///     .build()
///     .expect("KeyQueue creation failed");
/// // check if KeyQueue is not null
/// assert!(qb1.handle() != 0, "KeyQueue handle should not be zero");
/// ```
pub struct KeyQueueBuilder {
    k2h: u64,
    fifo: bool,
    prefix: Option<String>,
    password: Option<String>,
    expire_duration: Option<u64>,
}

impl KeyQueueBuilder {
    /// Create a new KeyQueueBuilder.
    pub fn new(k2h: u64) -> Self {
        KeyQueueBuilder {
            k2h,
            fifo: true,
            prefix: None,
            password: None,
            expire_duration: None,
        }
    }

    /// Set the FIFO mode for the KeyQueue.
    pub fn fifo(mut self, fifo: bool) -> Self {
        self.fifo = fifo;
        self
    }

    /// Set the prefix for the KeyQueue.
    pub fn prefix(mut self, prefix: String) -> Self {
        self.prefix = Some(prefix);
        self
    }

    /// Set the password for the KeyQueue.
    pub fn password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    /// Set the expire duration for the KeyQueue.
    pub fn expire_duration(mut self, expire_duration: u64) -> Self {
        self.expire_duration = Some(expire_duration);
        self
    }

    /// Create a new KeyQueue instance with options.
    pub fn build(self) -> Result<KeyQueue, &'static str> {
        KeyQueue::new(
            self.k2h,
            self.fifo,
            self.prefix,
            self.password,
            self.expire_duration,
        )
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
