use sensor_reader::{get_sensor_data, SensorData};
use std::env;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <parquet_file>", args[0]);
        std::process::exit(1);
    }
    let file_path = &args[1];

    println!("Verifying file: {}", file_path);

    let sensors_to_check = vec!["ch_0", "ch_999"];

    for sensor in sensors_to_check {
        println!("Reading sensor: {}", sensor);
        let reading_start = Instant::now();
        match get_sensor_data(file_path, sensor) {
            Ok(data) => match data {
                SensorData::Bit(v) => {
                    println!("  Type: Bit, Count: {}", v.len());
                    println!("  First 5: {:?}", &v[0..5.min(v.len())]);
                }
                SensorData::Int(v) => {
                    println!("  Type: Int, Count: {}", v.len());
                    println!("  First 5: {:?}", &v[0..5.min(v.len())]);
                }
                SensorData::Float(v) => {
                    println!("  Type: Float, Count: {}", v.len());
                    println!("  First 5: {:?}", &v[0..5.min(v.len())]);
                }
            },
            Err(e) => println!("  Error reading sensor: {}", e),
        }
        let reading_duration = reading_start.elapsed();
        println!("  Reading duration: {} ms", reading_duration.as_millis());
    }

    Ok(())
}
