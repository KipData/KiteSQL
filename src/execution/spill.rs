// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::DatabaseError;
use crate::types::value::DataValue;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_MAX_ROWS: usize = 1024;
const DEFAULT_MAX_BYTES: usize = 1024 * 1024;
static NEXT_SPILL_FILE_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) trait SpillCodec: Sized {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError>;

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError>;

    fn estimated_size(&self) -> usize;
}

impl SpillCodec for Vec<DataValue> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        let len: u32 = self.len().try_into()?;
        writer.write_all(&len.to_le_bytes())?;
        for value in self {
            value.encode_reference_value(writer)?;
        }
        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        let mut len = [0; size_of::<u32>()];
        reader.read_exact(&mut len)?;
        let len = u32::from_le_bytes(len) as usize;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(DataValue::decode_reference_value(reader)?);
        }
        Ok(values)
    }

    fn estimated_size(&self) -> usize {
        size_of::<Vec<DataValue>>()
            .saturating_add(self.capacity().saturating_mul(size_of::<DataValue>()))
            .saturating_add(
                self.iter()
                    .map(estimated_dynamic_value_size)
                    .fold(0usize, usize::saturating_add),
            )
    }
}

fn estimated_dynamic_value_size(value: &DataValue) -> usize {
    match value {
        DataValue::Utf8 { value, .. } => value.capacity(),
        DataValue::Tuple(values, _) => values
            .capacity()
            .saturating_mul(size_of::<DataValue>())
            .saturating_add(
                values
                    .iter()
                    .map(estimated_dynamic_value_size)
                    .fold(0usize, usize::saturating_add),
            ),
        _ => 0,
    }
}

pub(crate) struct SpillVec<T: SpillCodec> {
    writer: Result<WriteState<T>, DatabaseError>,
    max_rows: usize,
    max_bytes: usize,
}

pub(crate) struct SpillReader<T: SpillCodec> {
    state: ReadState<T>,
}

struct WriteState<T: SpillCodec> {
    buffer: Vec<T>,
    buffer_bytes: usize,
    file: Option<SpillFileWriter>,
}

enum ReadState<T: SpillCodec> {
    Memory(std::vec::IntoIter<T>),
    Spilled {
        file: SpillFileReader<T>,
        tail: std::vec::IntoIter<T>,
    },
    Failed(DatabaseError),
    Exhausted,
}

impl<T: SpillCodec> SpillVec<T> {
    pub(crate) fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES)
    }

    pub(crate) fn push(&mut self, value: T) -> Result<(), DatabaseError> {
        let state = self.writer.as_mut().map_err(|_| {
            DatabaseError::InvalidValue("cannot append to a failed SpillVec".to_string())
        })?;
        state.push(value, self.max_rows, self.max_bytes)
    }

    fn with_limits(max_rows: usize, max_bytes: usize) -> Self {
        assert!(max_rows > 0, "spill row limit must be positive");
        assert!(max_bytes > 0, "spill byte limit must be positive");
        Self {
            writer: Ok(WriteState {
                buffer: Vec::new(),
                buffer_bytes: 0,
                file: None,
            }),
            max_rows,
            max_bytes,
        }
    }
}

impl<T: SpillCodec> From<Vec<T>> for SpillVec<T> {
    fn from(values: Vec<T>) -> Self {
        let mut result = Self::new();
        for value in values {
            if let Err(error) = result.push(value) {
                result.writer = Err(error);
                break;
            }
        }
        result
    }
}

impl<T: SpillCodec> IntoIterator for SpillVec<T> {
    type Item = Result<T, DatabaseError>;
    type IntoIter = SpillReader<T>;

    fn into_iter(self) -> Self::IntoIter {
        let state = match self.writer {
            Ok(writer) => writer.into_read().unwrap_or_else(ReadState::Failed),
            Err(error) => ReadState::Failed(error),
        };
        SpillReader { state }
    }
}

impl<T: SpillCodec> Iterator for SpillReader<T> {
    type Item = Result<T, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if matches!(self.state, ReadState::Failed(_)) {
            let ReadState::Failed(error) = std::mem::replace(&mut self.state, ReadState::Exhausted)
            else {
                unreachable!()
            };
            return Some(Err(error));
        }

        let result = match &mut self.state {
            ReadState::Memory(rows) => Ok(rows.next()),
            ReadState::Spilled { file, tail } => {
                file.next().map(|value| value.or_else(|| tail.next()))
            }
            ReadState::Exhausted => return None,
            ReadState::Failed(_) => unreachable!(),
        };
        match result {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => {
                self.state = ReadState::Exhausted;
                None
            }
            Err(error) => {
                self.state = ReadState::Exhausted;
                Some(Err(error))
            }
        }
    }
}

impl<T: SpillCodec> WriteState<T> {
    fn push(&mut self, value: T, max_rows: usize, max_bytes: usize) -> Result<(), DatabaseError> {
        let value_size = value.estimated_size();
        self.buffer.push(value);
        self.buffer_bytes = self.buffer_bytes.saturating_add(value_size);

        let should_flush = self.buffer.len() >= max_rows || self.buffer_bytes >= max_bytes;
        if should_flush {
            self.start_spilling()?;
            self.flush()?;
        }
        Ok(())
    }

    fn start_spilling(&mut self) -> Result<(), DatabaseError> {
        if self.file.is_none() {
            self.file = Some(SpillFileWriter::new()?);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), DatabaseError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.file
            .as_mut()
            .expect("spill file initialized before flush")
            .append_segment(&self.buffer)?;
        self.buffer.clear();
        self.buffer_bytes = 0;
        Ok(())
    }

    fn into_read(mut self) -> Result<ReadState<T>, DatabaseError> {
        let Some(file) = self.file.take() else {
            return Ok(ReadState::Memory(self.buffer.into_iter()));
        };
        // Flushed segments are always a prefix; the in-memory buffer is its ordered tail.
        Ok(ReadState::Spilled {
            file: file.into_reader()?,
            tail: self.buffer.into_iter(),
        })
    }
}

#[derive(Clone, Copy)]
struct SegmentMeta {
    offset: u64,
    row_count: usize,
}

struct SpillFileGuard {
    path: PathBuf,
}

impl Drop for SpillFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct SpillFileWriter {
    file: File,
    segments: Vec<SegmentMeta>,
    file_guard: SpillFileGuard,
}

impl SpillFileWriter {
    fn new() -> Result<Self, DatabaseError> {
        let (file, path) = create_spill_file()?;
        Ok(Self {
            file,
            segments: Vec::new(),
            file_guard: SpillFileGuard { path },
        })
    }

    fn append_segment<T: SpillCodec>(&mut self, rows: &[T]) -> Result<(), DatabaseError> {
        let offset = self.file.stream_position()?;
        for row in rows {
            row.encode(&mut self.file)?;
        }
        let end = self.file.stream_position()?;
        self.segments.push(SegmentMeta {
            offset,
            row_count: rows.len(),
        });
        debug_assert!(end > offset);
        Ok(())
    }

    fn into_reader<T: SpillCodec>(mut self) -> Result<SpillFileReader<T>, DatabaseError> {
        self.file.flush()?;
        let file = File::open(&self.file_guard.path)?;
        Ok(SpillFileReader {
            file,
            segments: self.segments.into_iter(),
            current_segment_rows: 0,
            marker: PhantomData,
            _file_guard: self.file_guard,
        })
    }
}

struct SpillFileReader<T: SpillCodec> {
    file: File,
    segments: std::vec::IntoIter<SegmentMeta>,
    current_segment_rows: usize,
    marker: PhantomData<T>,
    _file_guard: SpillFileGuard,
}

impl<T: SpillCodec> SpillFileReader<T> {
    fn next(&mut self) -> Result<Option<T>, DatabaseError> {
        if self.current_segment_rows == 0 && !self.start_next_segment()? {
            return Ok(None);
        }

        let value = T::decode(&mut self.file)?;
        self.current_segment_rows -= 1;
        Ok(Some(value))
    }

    fn start_next_segment(&mut self) -> Result<bool, DatabaseError> {
        let Some(segment) = self.segments.next() else {
            return Ok(false);
        };
        self.current_segment_rows = segment.row_count;
        self.file.seek(SeekFrom::Start(segment.offset))?;
        Ok(true)
    }
}

fn create_spill_file() -> Result<(File, PathBuf), DatabaseError> {
    loop {
        let id = NEXT_SPILL_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("kitesql-spill-{}-{id}.tmp", std::process::id()));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(value: i32) -> Vec<DataValue> {
        vec![DataValue::Int32(value)]
    }

    #[test]
    fn small_spill_vec_transitions_to_memory_reading() -> Result<(), DatabaseError> {
        let mut values = SpillVec::with_limits(2, usize::MAX);
        values.push(row(1))?;

        let mut reader = values.into_iter();
        assert!(matches!(reader.state, ReadState::Memory(_)));
        assert_eq!(reader.next().transpose()?, Some(row(1)));
        assert_eq!(reader.next().transpose()?, None);
        Ok(())
    }

    #[test]
    fn push_automatically_spills_and_next_preserves_order() -> Result<(), DatabaseError> {
        let mut values = SpillVec::with_limits(2, usize::MAX);
        for value in 0..5 {
            values.push(row(value))?;
        }
        assert!(matches!(
            &values.writer,
            Ok(WriteState { file: Some(_), .. })
        ));

        let reader = values.into_iter();
        assert!(matches!(
            &reader.state,
            ReadState::Spilled { tail, .. } if tail.len() == 1
        ));
        let restored = reader.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(restored, (0..5).map(row).collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn spill_reader_stays_exhausted() -> Result<(), DatabaseError> {
        let mut reader = SpillVec::from(vec![row(1)]).into_iter();
        assert_eq!(reader.next().transpose()?, Some(row(1)));
        assert_eq!(reader.next().transpose()?, None);
        assert_eq!(reader.next().transpose()?, None);
        Ok(())
    }
}
