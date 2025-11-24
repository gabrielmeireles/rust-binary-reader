use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;

pub enum SensorData {
    Bit(Vec<u8>),
    Int(Vec<i32>),
    Float(Vec<f64>),
}

pub fn get_sensor_data<P: AsRef<Path>>(
    file_path: P,
    sensor_name: &str,
) -> Result<SensorData, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    // Find column index by name
    let schema = metadata.file_metadata().schema();
    let column_index = schema
        .get_fields()
        .iter()
        .position(|f| f.name() == sensor_name)
        .ok_or_else(|| format!("Sensor '{}' not found in file", sensor_name))?;

    // We need to read row groups and extract the column chunk
    // For simplicity, we'll read all row groups and concatenate the data
    // In a real scenario, we might want to stream this or read specific ranges

    // Determine type from the first row group (assuming consistent schema)
    // Actually, we can check the schema type
    // But let's just use the arrow reader for convenience?
    // The user requirement is "read that directly from the newly created data".
    // Using `parquet::arrow` is easier to get Arrow arrays which we can convert to Vec.

    // Let's switch to using Arrow reader which is higher level
    drop(reader); // Close file to reopen with arrow reader

    let file = File::open(file_path.as_ref())?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Project only the requested column
    // We also might want the timestamp? The user didn't explicitly ask for it with the sensor data,
    // but usually it's needed. For now, let's just return the sensor data as requested.

    // Check if column exists in arrow schema
    let arrow_schema = builder.schema();
    let _idx = arrow_schema.index_of(sensor_name)?;

    // Build reader with projection
    // We can use the mask to select columns
    let mask = parquet::arrow::ProjectionMask::named_roots(
        builder.parquet_schema(),
        vec![sensor_name.to_string()],
    );
    let mut reader = builder.with_projection(mask).build()?;

    let mut result_data: Option<SensorData> = None;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        let array = batch.column(0); // We only projected one column

        match array.data_type() {
            arrow::datatypes::DataType::UInt8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt8Array>()
                    .unwrap();
                let vec: Vec<u8> = values.values().to_vec();
                match &mut result_data {
                    Some(SensorData::Bit(v)) => v.extend(vec),
                    None => result_data = Some(SensorData::Bit(vec)),
                    _ => return Err("Type mismatch between batches".into()),
                }
            }
            arrow::datatypes::DataType::Int32 => {
                let values = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap();
                let vec: Vec<i32> = values.values().to_vec();
                match &mut result_data {
                    Some(SensorData::Int(v)) => v.extend(vec),
                    None => result_data = Some(SensorData::Int(vec)),
                    _ => return Err("Type mismatch between batches".into()),
                }
            }
            arrow::datatypes::DataType::Float64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                let vec: Vec<f64> = values.values().to_vec();
                match &mut result_data {
                    Some(SensorData::Float(v)) => v.extend(vec),
                    None => result_data = Some(SensorData::Float(vec)),
                    _ => return Err("Type mismatch between batches".into()),
                }
            }
            dt => return Err(format!("Unsupported data type: {:?}", dt).into()),
        }
    }

    result_data.ok_or_else(|| "No data found".into())
}
