
use std::fs::File;
use std::io::{BufReader, SeekFrom, Seek};
use arrow::json::reader::infer_json_schema;
use arrow::datatypes::Schema;
use std::io::prelude::*;

fn main() {
    // read input file
    let mut file = File::open("./data/input1.json").unwrap();
    // file's cursor's offset at 0
    let reader = BufReader::new(&file);
    // write the input schema to schema.txt file.
    write_schema_to_file(reader, "schema.txt");
    // seek back to start so that the original file is usable again
    file.seek(SeekFrom::Start(0)).unwrap();

    // Validation Flow
    // read second input file
    let second_file = File::open("./data/input2.json").unwrap();
    // file's cursor's offset at 0
    let mut second_reader = BufReader::new(&second_file);
    let input_schema = infer_json_schema(&mut second_reader, None).unwrap();
    read_schema_from_file("schema.txt");
    let schema = read_schema_from_file("schema.txt");
    assert_eq!(schema.to_json(), input_schema.to_json())
}

fn write_schema_to_file(mut reader: std::io::BufReader<&std::fs::File>, file_path: &str) {
    let inferred_schema = infer_json_schema(&mut reader, None).unwrap();  
    let schema_file = File::create(file_path).unwrap();
    write!(&schema_file, "{:?}", inferred_schema.to_json()).unwrap();
}

fn read_schema_from_file(file_path: &str)  -> arrow::datatypes::Schema {
    let schema_file = File::open(file_path).unwrap();
    let mut reader = BufReader::new(schema_file);
    let mut jsonstr = String::new();
    reader.read_to_string(&mut jsonstr).unwrap();
    println!("{:?}", jsonstr);
    let json = serde_json::from_str(&jsonstr).unwrap();
    Schema::from(&json).unwrap()
}