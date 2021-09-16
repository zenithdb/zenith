use std::{fs::File, io::Write};

use anyhow::{ensure, Result};
use bookfile::{BookWriter, BoundedReader, ChapterId, ChapterWriter};
use crc32c::crc32c;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct BlobRange {
    offset: u64,
    size: usize,
    cksum: u32,
}

pub fn read_blob(reader: &BoundedReader<&'_ File>, range: &BlobRange) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; range.size];
    reader.read_exact_at(&mut buf, range.offset)?;
    ensure!(range.cksum == crc32c(&buf));
    Ok(buf)
}

pub struct BlobWriter {
    writer: ChapterWriter<File>,
    offset: u64,
}

impl BlobWriter {
    // This function takes a BookWriter and creates a new chapter to ensure offset is 0.
    pub fn new(book_writer: BookWriter<File>, chapter_id: impl Into<ChapterId>) -> Self {
        let writer = book_writer.new_chapter(chapter_id);
        Self { writer, offset: 0 }
    }

    pub fn write_blob(&mut self, blob: &[u8]) -> Result<BlobRange> {
        self.writer.write_all(blob)?;

        let cksum = crc32c(blob);

        let range = BlobRange {
            offset: self.offset,
            size: blob.len(),
            cksum,
        };
        self.offset += blob.len() as u64;
        Ok(range)
    }

    pub fn close(self) -> bookfile::Result<BookWriter<File>> {
        self.writer.close()
    }
}
