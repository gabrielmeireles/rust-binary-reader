use arrow::array::{ArrayRef, Float64Array, Int32Array, UInt8Array};
use arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use binary_processor::{BatchReader, ChannelData, DataType, Schema};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input binary file
    #[arg(short, long, default_value = ".data/data.bin")]
    input: String,

    /// Output parquet file
    #[arg(short, long, default_value = ".data/output.parquet")]
    output: String,

    /// Schema file
    #[arg(short, long, default_value = ".data/schema.json")]
    schema: String,

    /// Memory limit in MB (approximate)
    #[arg(short, long, default_value_t = 1024)]
    memory_limit_mb: usize,
}

fn main() -> anyhow::Result<()> {
    let parsing_start = Instant::now();

    let args = Args::parse();

    println!("Reading schema from {}...", args.schema);
    let schema_content = std::fs::read_to_string(&args.schema)?;
    let schema: Schema = serde_json::from_str(&schema_content)?;

    println!("Initializing reader for {}...", args.input);
    let mut reader = BatchReader::new(&args.input, schema.clone())?;
    let total_rows = reader.total_rows();
    println!("Total rows found: {}", total_rows);

    // Calculate batch size based on memory limit
    // Row size in bytes
    let row_size_bytes = schema.row_size();
    // Target memory usage per batch (let's use 50% of limit for safety buffer)
    let target_batch_mem_bytes = (args.memory_limit_mb * 1024 * 1024) / 2;
    let batch_size = std::cmp::max(1, target_batch_mem_bytes / row_size_bytes);

    println!(
        "Memory limit: {} MB. Calculated batch size: {} rows.",
        args.memory_limit_mb, batch_size
    );

    // Setup Arrow Schema
    let mut fields = Vec::new();
    // Add Timestamp field
    fields.push(Field::new("timestamp", ArrowType::Float64, false));

    for channel in &schema.channels {
        let arrow_type = match channel.data_type {
            DataType::Bit => ArrowType::UInt8,
            DataType::Int => ArrowType::Int32,
            DataType::Float => ArrowType::Float64,
        };
        fields.push(Field::new(&channel.name, arrow_type, false));
    }
    let arrow_schema = Arc::new(ArrowSchema::new(fields));

    // Setup Parquet Writer
    let file = File::create(&args.output)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;

    let mut processed_rows = 0;

    while let Some(channels_data) = reader.read_batch(batch_size) {
        let current_batch_size = channels_data[0].len();
        if current_batch_size == 0 {
            break;
        }

        // Read timestamps for this batch
        let timestamps = reader.read_timestamps(processed_rows, current_batch_size);

        // Convert to Arrow Arrays
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(channels_data.len() + 1);

        // Add Timestamp column
        columns.push(Arc::new(Float64Array::from(timestamps)));

        for data in channels_data {
            let array: ArrayRef = match data {
                ChannelData::Bit(v) => Arc::new(UInt8Array::from(v)),
                ChannelData::Int(v) => Arc::new(Int32Array::from(v)),
                ChannelData::Float(v) => Arc::new(Float64Array::from(v)),
            };
            columns.push(array);
        }

        let batch = RecordBatch::try_new(arrow_schema.clone(), columns)?;
        writer.write(&batch)?;

        processed_rows += current_batch_size;
        println!(
            "Processed {} / {} rows ({:.1}%)",
            processed_rows,
            total_rows,
            (processed_rows as f64 / total_rows as f64) * 100.0
        );
    }

    writer.close()?;
    println!("Conversion complete. Output saved to {}", args.output);
    let parsing_duration = parsing_start.elapsed();
    println!("Parsing duration: {} ms", parsing_duration.as_millis());

    Ok(())
}
