use binary_processor::{ChannelData, DataType, Schema};
use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::MmapOptions;
use rayon::prelude::*;
use std::fs::File;
use std::io::Cursor;
use std::time::Instant;

fn read_channels(
    filename: &str,
    schema: &Schema,
) -> std::io::Result<(Vec<ChannelData>, std::time::Duration, std::time::Duration)> {
    let file = File::open(filename)?;

    // --- Phase 1: Mmap (Zero-copy I/O) ---
    let io_start = Instant::now();
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let io_duration = io_start.elapsed();

    // Calculate offsets
    let pre_skip = 8; // Timestamp
    let block_size = schema
        .channels
        .iter()
        .map(|c| c.data_type.size())
        .sum::<usize>();

    let row_size = pre_skip + block_size;

    let total_rows = mmap.len() / row_size;

    // --- Phase 2: Parallel Parsing ---
    let parse_start = Instant::now();

    let num_channels = schema.channels.len();
    let chunk_size = 10_000;
    let num_chunks = (total_rows + chunk_size - 1) / chunk_size;

    let chunks: Vec<(usize, usize)> = (0..num_chunks)
        .map(|i| {
            let start_row = i * chunk_size;
            let end_row = std::cmp::min((i + 1) * chunk_size, total_rows);
            (start_row, end_row)
        })
        .collect();

    let partial_results: Vec<Vec<ChannelData>> = chunks
        .par_iter()
        .map(|&(start_row, end_row)| {
            let rows_in_chunk = end_row - start_row;
            // Initialize mini-columns
            let mut chunk_results = Vec::with_capacity(num_channels);
            for i in 0..num_channels {
                match schema.channels[i].data_type {
                    DataType::Bit => {
                        chunk_results.push(ChannelData::Bit(Vec::with_capacity(rows_in_chunk)))
                    }
                    DataType::Int => {
                        chunk_results.push(ChannelData::Int(Vec::with_capacity(rows_in_chunk)))
                    }
                    DataType::Float => {
                        chunk_results.push(ChannelData::Float(Vec::with_capacity(rows_in_chunk)))
                    }
                }
            }

            // Parse rows in this chunk
            let mut offset = start_row * row_size + pre_skip;

            for _ in 0..rows_in_chunk {
                let block_end = offset + block_size;
                let block_slice = &mmap[offset..block_end];

                let mut cursor = Cursor::new(block_slice);

                for i in 0..num_channels {
                    match &mut chunk_results[i] {
                        ChannelData::Bit(vec) => vec.push(cursor.read_u8().unwrap()),
                        ChannelData::Int(vec) => {
                            vec.push(cursor.read_i32::<LittleEndian>().unwrap())
                        }
                        ChannelData::Float(vec) => {
                            vec.push(cursor.read_f64::<LittleEndian>().unwrap())
                        }
                    }
                }

                offset += row_size;
            }
            chunk_results
        })
        .collect();

    // Merge results
    let mut final_results = Vec::with_capacity(num_channels);

    // Initialize final vectors
    for i in 0..num_channels {
        match schema.channels[i].data_type {
            DataType::Bit => final_results.push(ChannelData::Bit(Vec::with_capacity(total_rows))),
            DataType::Int => final_results.push(ChannelData::Int(Vec::with_capacity(total_rows))),
            DataType::Float => {
                final_results.push(ChannelData::Float(Vec::with_capacity(total_rows)))
            }
        }
    }

    // Flatten/Extend
    for chunk_res in partial_results {
        for (i, channel_data) in chunk_res.into_iter().enumerate() {
            match (&mut final_results[i], channel_data) {
                (ChannelData::Bit(dest), ChannelData::Bit(src)) => dest.extend(src),
                (ChannelData::Int(dest), ChannelData::Int(src)) => dest.extend(src),
                (ChannelData::Float(dest), ChannelData::Float(src)) => dest.extend(src),
                _ => unreachable!("Type mismatch during merge"),
            }
        }
    }

    let parse_duration = parse_start.elapsed();

    Ok((final_results, io_duration, parse_duration))
}

fn main() -> std::io::Result<()> {
    // Load schema
    let schema_content = std::fs::read_to_string("schema.json")?;
    let schema: Schema = serde_json::from_str(&schema_content)?;

    println!("Reading all channels...");

    let start = Instant::now();

    let (channels_data, io_time, parse_time) = read_channels("data.bin", &schema)?;

    let total_duration = start.elapsed();
    let num_rows = channels_data[0].len();
    let speed_rows_per_ms = num_rows as f64 / total_duration.as_millis() as f64;

    println!("--------------------------------------------------");
    println!(
        "Read {} rows for {} channels.",
        num_rows,
        channels_data.len()
    );
    println!("I/O Time (Reading file): {:?}", io_time);
    println!("Parse Time (Bytes -> Numbers): {:?}", parse_time);
    println!("Total Time (including overhead): {:?}", total_duration);
    println!(
        "Speed (with {} channels) (rows per ms): {}",
        channels_data.len(),
        speed_rows_per_ms.floor()
    );
    println!("--------------------------------------------------");

    // Verify structure: Print first 5 values of the first and last channel read
    if !channels_data.is_empty() {
        // Helper to print first 5 elements of a ChannelData
        let print_first_5 = |data: &ChannelData| match data {
            ChannelData::Bit(v) => println!("{:?}", &v[0..5.min(v.len())]),
            ChannelData::Int(v) => println!("{:?}", &v[0..5.min(v.len())]),
            ChannelData::Float(v) => println!("{:?}", &v[0..5.min(v.len())]),
        };

        print!("Channel {} (first read channel): ", 0);
        print_first_5(&channels_data[0]);

        print!(
            "Channel {} (last read channel): ",
            schema.channels.len() - 1
        );
        print_first_5(&channels_data[channels_data.len() - 1]);
    }

    Ok(())
}
