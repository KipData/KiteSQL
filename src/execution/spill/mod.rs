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
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

mod codec;

pub(crate) use codec::SortRow;

const DEFAULT_MAX_ROWS: usize = 1024;
const DEFAULT_MAX_BYTES: usize = 1024 * 1024;
static NEXT_SPILL_FILE_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) type SegmentOffset = u64;

pub(crate) trait SpillCodec: Sized {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError>;

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError>;

    fn estimated_size(&self) -> usize;
}

pub(crate) struct SpillVec<'on_flush, T: SpillCodec> {
    writer: Result<WriteState<'on_flush, T>, DatabaseError>,
    max_rows: usize,
    max_bytes: usize,
}

pub(crate) struct SpillReader<T: SpillCodec> {
    state: ReadState<T>,
}

struct WriteState<'on_flush, T: SpillCodec> {
    buffer: Vec<T>,
    buffer_bytes: usize,
    file: Option<SpillFileWriter>,
    on_flush: Option<OnFlush<'on_flush, T>>,
}

type OnFlush<'on_flush, T> = Box<dyn FnMut(&mut Vec<T>) -> Result<(), DatabaseError> + 'on_flush>;

enum ReadState<T: SpillCodec> {
    Memory(std::vec::IntoIter<T>),
    Spilled {
        reader: SegmentReader<'static, File, T>,
        tail: std::vec::IntoIter<T>,
        _file_guard: SpillFileGuard,
    },
    Failed(DatabaseError),
    Exhausted,
}

impl<'on_flush, T: SpillCodec> SpillVec<'on_flush, T> {
    pub(crate) fn new() -> Self {
        Self {
            writer: Ok(WriteState {
                buffer: Vec::new(),
                buffer_bytes: 0,
                file: None,
                on_flush: None,
            }),
            max_rows: DEFAULT_MAX_ROWS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }

    #[cfg(test)]
    pub(crate) fn limit(mut self, max_rows: usize, max_bytes: usize) -> Self {
        assert!(max_rows > 0, "spill row limit must be positive");
        assert!(max_bytes > 0, "spill byte limit must be positive");
        self.max_rows = max_rows;
        self.max_bytes = max_bytes;
        self
    }

    pub(crate) fn on_flush<F>(mut self, on_flush: F) -> Self
    where
        F: FnMut(&mut Vec<T>) -> Result<(), DatabaseError> + 'on_flush,
    {
        if let Ok(state) = &mut self.writer {
            state.on_flush = Some(Box::new(on_flush));
        }
        self
    }

    pub(crate) fn push(&mut self, value: T) -> Result<Option<SegmentOffset>, DatabaseError> {
        let state = self.writer.as_mut().map_err(|_| {
            DatabaseError::InvalidValue("cannot append to a failed SpillVec".to_string())
        })?;
        state.push(value, self.max_rows, self.max_bytes)
    }

    pub(crate) fn is_spilled(&self) -> bool {
        matches!(&self.writer, Ok(state) if state.file.is_some())
    }

    pub(crate) fn flush(&mut self) -> Result<Option<SegmentOffset>, DatabaseError> {
        let state = self.writer.as_mut().map_err(|_| {
            DatabaseError::InvalidValue("cannot flush a failed SpillVec".to_string())
        })?;
        if state.buffer.is_empty() {
            return Ok(None);
        }
        state.start_spilling()?;
        state.flush()
    }
}

impl<'on_flush, T: SpillCodec> From<Vec<T>> for SpillVec<'on_flush, T> {
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

impl<T: SpillCodec> IntoIterator for SpillVec<'_, T> {
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
            ReadState::Spilled { reader, tail, .. } => loop {
                match reader.next() {
                    Some(Ok(value)) => break Ok(Some(value)),
                    Some(Err(error)) => break Err(error),
                    None => match reader.start_next_segment() {
                        Ok(true) => continue,
                        Ok(false) => break Ok(tail.next()),
                        Err(error) => break Err(error),
                    },
                }
            },
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

impl<T: SpillCodec> SpillReader<T> {
    pub(crate) fn open_segment_reader<'source>(
        &'source self,
    ) -> Result<SegmentReader<'source, BufReader<File>, T>, DatabaseError> {
        let ReadState::Spilled { _file_guard, .. } = &self.state else {
            return Err(DatabaseError::InvalidValue(
                "cannot open a segment reader for an in-memory SpillVec".to_string(),
            ));
        };
        Ok(SegmentReader::new(BufReader::new(File::open(
            &_file_guard.path,
        )?)))
    }
}

pub(crate) struct SegmentReader<'source, R, T: SpillCodec> {
    reader: R,
    remaining_rows: usize,
    marker: PhantomData<T>,
    source: PhantomData<&'source SpillFileGuard>,
}

impl<R, T: SpillCodec> SegmentReader<'_, R, T> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            remaining_rows: 0,
            marker: PhantomData,
            source: PhantomData,
        }
    }

    fn is_exhausted(&self) -> bool {
        self.remaining_rows == 0
    }
}

impl<R: Read, T: SpillCodec> SegmentReader<'_, R, T> {
    pub(crate) fn start_next_segment(&mut self) -> Result<bool, DatabaseError> {
        debug_assert!(self.is_exhausted());
        let mut row_count = [0; size_of::<u64>()];
        if self.reader.read(&mut row_count[..1])? == 0 {
            return Ok(false);
        }
        self.reader.read_exact(&mut row_count[1..])?;
        self.remaining_rows = u64::from_le_bytes(row_count).try_into()?;
        if self.remaining_rows == 0 {
            return Err(DatabaseError::InvalidValue(
                "spill segment cannot be empty".to_string(),
            ));
        }
        Ok(true)
    }
}

impl<R: Read + Seek, T: SpillCodec> SegmentReader<'_, R, T> {
    pub(crate) fn reset(&mut self, offset: SegmentOffset) -> Result<(), DatabaseError> {
        self.reader.seek(SeekFrom::Start(offset))?;
        self.remaining_rows = 0;
        if !self.start_next_segment()? {
            return Err(DatabaseError::InvalidValue(
                "spill segment offset points to end of file".to_string(),
            ));
        }
        Ok(())
    }
}

impl<R: Read, T: SpillCodec> Iterator for SegmentReader<'_, R, T> {
    type Item = Result<T, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_rows == 0 {
            return None;
        }
        match T::decode(&mut self.reader) {
            Ok(value) => {
                self.remaining_rows -= 1;
                Some(Ok(value))
            }
            Err(error) => {
                self.remaining_rows = 0;
                Some(Err(error))
            }
        }
    }
}

impl<T: SpillCodec> WriteState<'_, T> {
    fn push(
        &mut self,
        value: T,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Option<SegmentOffset>, DatabaseError> {
        let value_size = value.estimated_size();
        self.buffer.push(value);
        self.buffer_bytes = self.buffer_bytes.saturating_add(value_size);

        if self.buffer.len() >= max_rows || self.buffer_bytes >= max_bytes {
            self.start_spilling()?;
            return self.flush();
        }
        Ok(None)
    }

    fn start_spilling(&mut self) -> Result<(), DatabaseError> {
        if self.file.is_none() {
            self.file = Some(SpillFileWriter::new()?);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<SegmentOffset>, DatabaseError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        if let Some(on_flush) = &mut self.on_flush {
            on_flush(&mut self.buffer)?;
        }
        let Some(file) = self.file.as_mut() else {
            return Err(DatabaseError::InvalidValue(
                "cannot flush without a spill file".to_string(),
            ));
        };
        let segment = file.append_segment(&self.buffer)?;
        self.buffer.clear();
        self.buffer_bytes = 0;
        Ok(Some(segment))
    }

    fn into_read(mut self) -> Result<ReadState<T>, DatabaseError> {
        if let Some(on_flush) = &mut self.on_flush {
            on_flush(&mut self.buffer)?;
        }
        let Some(file) = self.file.take() else {
            return Ok(ReadState::Memory(self.buffer.into_iter()));
        };
        file.into_reader(self.buffer.into_iter())
    }
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
    file_guard: SpillFileGuard,
}

impl SpillFileWriter {
    fn new() -> Result<Self, DatabaseError> {
        let (file, path) = create_spill_file()?;
        Ok(Self {
            file,
            file_guard: SpillFileGuard { path },
        })
    }

    fn append_segment<T: SpillCodec>(
        &mut self,
        rows: &[T],
    ) -> Result<SegmentOffset, DatabaseError> {
        let offset = self.file.stream_position()?;
        // The row-count header makes segment boundaries discoverable without an in-memory index.
        let row_count: u64 = rows.len().try_into()?;
        self.file.write_all(&row_count.to_le_bytes())?;
        for row in rows {
            row.encode(&mut self.file)?;
        }
        let end = self.file.stream_position()?;
        debug_assert!(end > offset);
        Ok(offset)
    }

    fn into_reader<T: SpillCodec>(
        mut self,
        tail: std::vec::IntoIter<T>,
    ) -> Result<ReadState<T>, DatabaseError> {
        self.file.flush()?;
        let file = File::open(&self.file_guard.path)?;
        // Flushed segments are always a prefix; the in-memory buffer is its ordered tail.
        Ok(ReadState::Spilled {
            reader: SegmentReader::new(file),
            tail,
            _file_guard: self.file_guard,
        })
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
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;

    fn row(value: i32) -> Vec<DataValue> {
        vec![DataValue::Int32(value)]
    }

    #[test]
    fn small_spill_vec_transitions_to_memory_reading() -> Result<(), DatabaseError> {
        let mut values = SpillVec::new().limit(2, usize::MAX);
        let _ = values.push(row(1))?;

        let mut reader = values.into_iter();
        assert!(matches!(reader.state, ReadState::Memory(_)));
        assert_eq!(reader.next().transpose()?, Some(row(1)));
        assert_eq!(reader.next().transpose()?, None);
        Ok(())
    }

    #[test]
    fn push_automatically_spills_and_next_preserves_order() -> Result<(), DatabaseError> {
        let mut values = SpillVec::new().limit(2, usize::MAX);
        for value in 0..5 {
            let _ = values.push(row(value))?;
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

    #[test]
    fn spill_reader_opens_independent_segment_readers() -> Result<(), DatabaseError> {
        let mut values = SpillVec::new().limit(usize::MAX, usize::MAX);
        let _ = values.push(row(1))?;
        let _ = values.push(row(2))?;
        let first = values.flush()?.ok_or_else(|| {
            DatabaseError::InvalidValue("expected first spill segment".to_string())
        })?;
        let _ = values.push(row(3))?;
        let second = values.flush()?.ok_or_else(|| {
            DatabaseError::InvalidValue("expected second spill segment".to_string())
        })?;

        let source = values.into_iter();
        let mut first_reader = source.open_segment_reader()?;
        first_reader.reset(first)?;
        let mut second_reader = source.open_segment_reader()?;
        second_reader.reset(second)?;

        assert_eq!(first_reader.next().transpose()?, Some(row(1)));
        assert_eq!(second_reader.next().transpose()?, Some(row(3)));
        assert_eq!(first_reader.next().transpose()?, Some(row(2)));
        assert_eq!(second_reader.next().transpose()?, None);
        assert_eq!(first_reader.next().transpose()?, None);
        Ok(())
    }

    #[test]
    fn on_flush_runs_before_memory_read_and_segment_flush() -> Result<(), DatabaseError> {
        fn reverse(rows: &mut [Vec<DataValue>]) -> Result<(), DatabaseError> {
            rows.reverse();
            Ok(())
        }

        let mut memory = SpillVec::new()
            .limit(3, usize::MAX)
            .on_flush(|rows| reverse(rows));
        let _ = memory.push(row(1))?;
        let _ = memory.push(row(2))?;
        assert_eq!(
            memory.into_iter().collect::<Result<Vec<_>, _>>()?,
            vec![row(2), row(1)]
        );

        let mut spilled = SpillVec::new()
            .limit(2, usize::MAX)
            .on_flush(|rows| reverse(rows));
        for value in 1..=4 {
            let _ = spilled.push(row(value))?;
        }
        assert_eq!(
            spilled.into_iter().collect::<Result<Vec<_>, _>>()?,
            vec![row(2), row(1), row(4), row(3)]
        );
        Ok(())
    }

    #[test]
    fn tuple_codec_preserves_primary_key_and_values() -> Result<(), DatabaseError> {
        let tuple = Tuple::new(
            Some(DataValue::Int32(7)),
            vec![DataValue::Null, DataValue::Int32(11)],
        );
        let mut bytes = Vec::new();
        tuple.encode(&mut bytes)?;
        assert_eq!(Tuple::decode(&mut bytes.as_slice())?, tuple);
        Ok(())
    }
}
