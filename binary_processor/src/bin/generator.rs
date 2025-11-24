use binary_processor::{Channel, DataType, Schema};
use byteorder::{LittleEndian, WriteBytesExt};
use rand::Rng;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> std::io::Result<()> {
    // Generate Schema with 1000 channels
    let mut channels = Vec::new();
    for i in 0..1000 {
        let data_type = match i % 3 {
            0 => DataType::Bit,
            1 => DataType::Int,
            _ => DataType::Float,
        };
        channels.push(Channel {
            name: format!("ch_{}", i),
            data_type,
        });
    }
    let schema = Schema { channels };

    // Save schema
    let schema_json = serde_json::to_string_pretty(&schema)?;
    std::fs::write("schema.json", schema_json)?;
    println!("Generated schema.json with 1000 channels");

    let file = File::create("data.bin")?;
    let mut writer = BufWriter::new(file);
    let mut rng = rand::thread_rng();

    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64;

    println!("Generating 1,000,000 rows with 1000 channels...");

    for i in 0..1_000_000 {
        // Write timestamp (8 bytes)
        writer.write_f64::<LittleEndian>(start_time + i as f64)?;

        for channel in &schema.channels {
            match channel.data_type {
                DataType::Bit => {
                    let val: u8 = rng.gen_range(0..=1);
                    writer.write_u8(val)?;
                }
                DataType::Int => {
                    let val: i32 = rng.gen();
                    writer.write_i32::<LittleEndian>(val)?;
                }
                DataType::Float => {
                    let val: f64 = rng.gen();
                    writer.write_f64::<LittleEndian>(val)?;
                }
            }
        }
    }

    writer.flush()?;
    println!("Done! Generated data.bin");
    Ok(())
}
