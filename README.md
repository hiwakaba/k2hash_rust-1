# k2hash_rust

## Overview
[![crates.io](https://img.shields.io/crates/v/k2hash_rust.svg)](https://crates.io/crates/k2hash_rust)
[![Documentation](https://docs.rs/k2hash_rust/badge.svg)](https://docs.rs/k2hash_rust)
[![CI](https://github.com/yahoojapan/k2hash_rust/actions/workflows/rust.yml/badge.svg)](https://github.com/yahoojapan/k2hash_rust/actions/workflows/rust.yml)

**k2hash_rust** implements a [k2hash](https://k2hash.antpick.ax/) client in Rust.

![k2hash Rust](https://raw.githubusercontent.com/yahoojapan/k2hash_rust/main/images/top_k2hash_rust.png)

## Install

Firstly you must install the [k2hash](https://k2hash.antpick.ax/) shared library.
```sh
curl -o- https://raw.github.com/yahoojapan/k2hash_rust/main/utils/libk2hash.sh | bash
```
You can install **k2hash** library step by step from [source code](https://github.com/yahoojapan/k2hash). See [Build](https://k2hash.antpick.ax/build.html) for details.

Download the **k2hash_rust** package.

```sh
cargo install k2hash_rust
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

Let's run eamples!
```
cargo run --example basic_usage
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
