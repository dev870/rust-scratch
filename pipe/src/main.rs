use pipe;
use std::io::{Read, Write};
use std::thread::spawn;

fn main() {
    let (mut read, mut write) = pipe::pipe();

    let message = "Hello, world!";
    spawn(move || write.write_all(message.as_bytes()).unwrap());

    let mut s = String::new();
    read.read_to_string(&mut s).unwrap();

    print!("{:?}", s);
    assert_eq!(&s, message);
}
