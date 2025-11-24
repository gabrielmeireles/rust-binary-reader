use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Cursor;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Bit,   // 1 byte
    Int,   // 4 bytes (i32)
    Float, // 8 bytes (f64)
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Bit => 1,
            DataType::Int => 4,
            DataType::Float => 8,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Channel {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Schema {
    pub channels: Vec<Channel>,
}

impl Schema {
    pub fn row_size(&self) -> usize {
        // Timestamp (8 bytes) + sum of channel sizes
        8 + self
            .channels
            .iter()
            .map(|c| c.data_type.size())
            .sum::<usize>()
    }
}

#[derive(Debug)]
pub enum ChannelData {
    Bit(Vec<u8>),
    Int(Vec<i32>),
    Float(Vec<f64>),
}

impl ChannelData {
    pub fn len(&self) -> usize {
        match self {
            ChannelData::Bit(v) => v.len(),
            ChannelData::Int(v) => v.len(),
            ChannelData::Float(v) => v.len(),
        }
    }
}

pub struct BatchReader {
    mmap: Mmap,
    schema: Schema,
    row_size: usize,
    total_rows: usize,
    current_row: usize,
}

impl BatchReader {
    pub fn new(filename: &str, schema: Schema) -> std::io::Result<Self> {
        let file = File::open(filename)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let row_size = schema.row_size();
        let total_rows = mmap.len() / row_size;

        Ok(Self {
            mmap,
            schema,
            row_size,
            total_rows,
            current_row: 0,
        })
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    pub fn read_batch(&mut self, batch_size: usize) -> Option<Vec<ChannelData>> {
        if self.current_row >= self.total_rows {
            return None;
        }

        let rows_to_read = std::cmp::min(batch_size, self.total_rows - self.current_row);
        let start_row = self.current_row;
        let end_row = start_row + rows_to_read;

        let num_channels = self.schema.channels.len();
        let mut batch_results = Vec::with_capacity(num_channels);

        // Initialize vectors for this batch
        for channel in &self.schema.channels {
            match channel.data_type {
                DataType::Bit => {
                    batch_results.push(ChannelData::Bit(Vec::with_capacity(rows_to_read)))
                }
                DataType::Int => {
                    batch_results.push(ChannelData::Int(Vec::with_capacity(rows_to_read)))
                }
                DataType::Float => {
                    batch_results.push(ChannelData::Float(Vec::with_capacity(rows_to_read)))
                }
            }
        }

        let mut offset = start_row * self.row_size + 8; // Skip timestamp for now, or read it if needed.
                                                        // Wait, the user requirement says "read this data".
                                                        // The original reader skipped timestamp (pre_skip = 8).
                                                        // Let's assume we need to read channels.
                                                        // If we need timestamp, we should add it. For now, let's stick to channels as per original reader.
                                                        // Actually, for "putting it into a better format", we PROBABLY want the timestamp too.
                                                        // But the schema doesn't have a timestamp field. It's implicit.
                                                        // Let's add a Timestamp channel implicitly or just handle it.
                                                        // For now, I will stick to the schema channels to match the original logic,
                                                        // but I should probably add a Timestamp column to the output parquet.
                                                        // Let's modify this to return Timestamp as well?
                                                        // The user said "read this data and then put it into a better format".
                                                        // Timestamps are data.

        for idx in 0..rows_to_read {
            let row_start = (start_row + idx) * self.row_size;
            let mut cursor = Cursor::new(&self.mmap[row_start..row_start + self.row_size]);

            // Skip timestamp (8 bytes)
            let _ = cursor.read_f64::<LittleEndian>().unwrap();

            for i in 0..num_channels {
                match &mut batch_results[i] {
                    ChannelData::Bit(vec) => vec.push(cursor.read_u8().unwrap()),
                    ChannelData::Int(vec) => vec.push(cursor.read_i32::<LittleEndian>().unwrap()),
                    ChannelData::Float(vec) => vec.push(cursor.read_f64::<LittleEndian>().unwrap()),
                }
            }
        }

        self.current_row += rows_to_read;
        Some(batch_results)
    }

    // Helper to read timestamps if we want them separately
    pub fn read_timestamps(&self, start_row: usize, count: usize) -> Vec<f64> {
        let mut timestamps = Vec::with_capacity(count);
        for i in 0..count {
            if start_row + i >= self.total_rows {
                break;
            }
            let offset = (start_row + i) * self.row_size;
            let ts = (&self.mmap[offset..offset + 8])
                .read_f64::<LittleEndian>()
                .unwrap();
            timestamps.push(ts);
        }
        timestamps
    }
}
