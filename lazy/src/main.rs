use lazy_static::lazy_static; // 1.4.0
use std::sync::RwLock;

lazy_static! {
    static ref ARRAY: RwLock<u8> = RwLock::new<u8>;
}

fn do_a_call() {
    let mut w = ARRAY.write().unwrap();
    *w += 1;
}

fn main() {
    do_a_call();
    do_a_call();
    do_a_call();

    println!("called {}", ARRAY.read().unwrap().len());
}