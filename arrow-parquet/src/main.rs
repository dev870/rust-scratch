
use std::fs::File;
use std::io::{BufReader, SeekFrom, Seek};
use arrow::json::reader::infer_json_schema;
use std::sync::Arc;
use arrow::json;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::SerializedFileReader;
use parquet::arrow::{ParquetFileArrowReader, ArrowReader};

fn main() {
        let mut file = File::open("./data/input1.json").unwrap();
        let mut reader = BufReader::new(&file);
        let inferred_schema = infer_json_schema(&mut reader, None).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut json = json::Reader::new(BufReader::new(file), Arc::new(inferred_schema), 1024, None);
        let batch = json.next().unwrap().unwrap();
        
        let mut file2 = File::open("./data/input2.json").unwrap();
        let mut reader2 = BufReader::new(&file2);
        let inferred_schema2 = infer_json_schema(&mut reader2, None).unwrap();
        file2.seek(SeekFrom::Start(0)).unwrap();
        let mut json2 = json::Reader::new(BufReader::new(file2), Arc::new(inferred_schema2), 1024, None);
        let batch2 = json2.next().unwrap().unwrap();
        
        file2 = File::open("./data/input2.json").unwrap();
        let mut reader2 = BufReader::new(&file2);
        let inferred_schema3 = infer_json_schema(&mut reader2, None).unwrap();
        file2.seek(SeekFrom::Start(0)).unwrap();     
        let vec = vec![batch, batch2];

        let new_batch = RecordBatch::concat(&Arc::new(inferred_schema3), &vec).unwrap();
        println!("{}", new_batch.num_columns());
        println!("{}", new_batch.num_rows());

        // let parquet_file = File::create("./data/data.parquet").unwrap();
        // let mut file1 = File::open("./data/input1.json").unwrap();
        // let mut reader1 = BufReader::new(&file1);
        // let inferred_schema_clone = infer_json_schema(&mut reader1, None).unwrap();
        // let props = WriterProperties::builder().build();
        // let mut writer = ArrowWriter::try_new(parquet_file, Arc::new(inferred_schema_clone), Some(props)).unwrap();
        // writer.write(&batch).expect("Writing batch");
        // writer.close().unwrap();

        // let file = File::open("./data/data.parquet").unwrap();
        // let file_reader = SerializedFileReader::new(file).unwrap();
        // let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
 
        // println!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());
        // println!("Arrow schema after projection is: {}",
        // arrow_reader.get_schema_by_columns(vec![0], true).unwrap());

        // let mut record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();

        // for maybe_record_batch in record_batch_reader {
        //     let record_batch = maybe_record_batch.unwrap();
        //         if record_batch.num_rows() > 0 {
        //             println!("Read {} records.", record_batch.num_rows());
        //         } else {
        //             println!("End of file!");
        //         }
        //         println!("{}", record_batch.num_columns());
        //         println!("{}", record_batch.num_rows());
        // }
}
