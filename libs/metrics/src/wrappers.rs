use std::io::{Read, Result, Write};

/// A wrapper for an object implementing [Read](std::io::Read)
/// which allows a closure to observe the amount of bytes read.
/// This is useful in conjunction with metrics (e.g. [IntCounter](crate::IntCounter)).
///
/// Example:
///
/// ```
/// # use std::io::{Result, Read};
/// # use metrics::{register_int_counter, IntCounter};
/// # use metrics::CountedReader;
/// # use once_cell::sync::Lazy;
/// #
/// # static INT_COUNTER: Lazy<IntCounter> = Lazy::new( || { register_int_counter!(
/// #         "int_counter",
/// #         "let's count something!"
/// #     ).unwrap()
/// # });
/// #
/// fn do_some_reads(stream: impl Read, count: usize) -> Result<Vec<u8>> {
///     let mut reader = CountedReader::new(stream, |cnt| {
///         // bump a counter each time we do a read
///         INT_COUNTER.inc_by(cnt as u64);
///     });
///
///     let mut proto_header = [0; 8];
///     reader.read_exact(&mut proto_header)?;
///     assert!(&proto_header == b"deadbeef");
///
///     let mut payload = vec![0; count];
///     reader.read_exact(&mut payload)?;
///     Ok(payload)
/// }
/// ```
///
/// NB: rapid concurrent bumping of an atomic counter might incur
/// a performance penalty. Please make sure to amortize the amount
/// of atomic operations by either using [BufReader](std::io::BufReader)
/// or choosing a non-atomic (thread local) counter.
pub struct CountedReader<'a, T> {
    reader: T,
    update_counter: Box<dyn FnMut(usize) + Sync + Send + 'a>,
}

impl<'a, T> CountedReader<'a, T> {
    pub fn new(reader: T, update_counter: impl FnMut(usize) + Sync + Send + 'a) -> Self {
        Self {
            reader,
            update_counter: Box::new(update_counter),
        }
    }

    /// Get an immutable reference to the underlying [Read](std::io::Read) implementer
    pub fn inner(&self) -> &T {
        &self.reader
    }

    /// Get a mutable reference to the underlying [Read](std::io::Read) implementer
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.reader
    }

    /// Consume the wrapper and return the underlying [Read](std::io::Read) implementer
    pub fn into_inner(self) -> T {
        self.reader
    }
}

impl<T: Read> Read for CountedReader<'_, T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let count = self.reader.read(buf)?;
        (self.update_counter)(count);
        Ok(count)
    }
}

/// A wrapper for an object implementing [Write](std::io::Write)
/// which allows a closure to observe the amount of bytes written.
/// This is useful in conjunction with metrics (e.g. [IntCounter](crate::IntCounter)).
///
/// Example:
///
/// ```
/// # use std::io::{Result, Write};
/// # use metrics::{register_int_counter, IntCounter};
/// # use metrics::CountedWriter;
/// # use once_cell::sync::Lazy;
/// #
/// # static INT_COUNTER: Lazy<IntCounter> = Lazy::new( || { register_int_counter!(
/// #         "int_counter",
/// #         "let's count something!"
/// #     ).unwrap()
/// # });
/// #
/// fn do_some_writes(stream: impl Write, payload: &[u8]) -> Result<()> {
///     let mut writer = CountedWriter::new(stream, |cnt| {
///         // bump a counter each time we do a write
///         INT_COUNTER.inc_by(cnt as u64);
///     });
///
///     let proto_header = b"deadbeef";
///     writer.write_all(proto_header)?;
///     writer.write_all(payload)
/// }
/// ```
///
/// NB: rapid concurrent bumping of an atomic counter might incur
/// a performance penalty. Please make sure to amortize the amount
/// of atomic operations by either using [BufWriter](std::io::BufWriter)
/// or choosing a non-atomic (thread local) counter.
pub struct CountedWriter<'a, T> {
    writer: T,
    update_counter: Box<dyn FnMut(usize) + Sync + Send + 'a>,
}

impl<'a, T> CountedWriter<'a, T> {
    pub fn new(writer: T, update_counter: impl FnMut(usize) + Sync + Send + 'a) -> Self {
        Self {
            writer,
            update_counter: Box::new(update_counter),
        }
    }

    /// Get an immutable reference to the underlying [Write](std::io::Write) implementer
    pub fn inner(&self) -> &T {
        &self.writer
    }

    /// Get a mutable reference to the underlying [Write](std::io::Write) implementer
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.writer
    }

    /// Consume the wrapper and return the underlying [Write](std::io::Write) implementer
    pub fn into_inner(self) -> T {
        self.writer
    }
}

impl<T: Write> Write for CountedWriter<'_, T> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let count = self.writer.write(buf)?;
        (self.update_counter)(count);
        Ok(count)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counted_reader() {
        let stream = [0; 16];
        let mut total = 0;
        let mut reader = CountedReader::new(stream.as_ref(), |cnt| {
            total += cnt;
        });

        let mut buffer = [0; 8];
        reader.read_exact(&mut buffer).unwrap();
        reader.read_exact(&mut buffer).unwrap();

        drop(reader);
        assert_eq!(total, stream.len());
    }

    #[test]
    fn test_counted_writer() {
        let mut stream = [0; 16];
        let mut total = 0;
        let mut writer = CountedWriter::new(stream.as_mut(), |cnt| {
            total += cnt;
        });

        let buffer = [0; 8];
        writer.write_all(&buffer).unwrap();
        writer.write_all(&buffer).unwrap();

        drop(writer);
        assert_eq!(total, stream.len());
    }

    // This mimics the constraints of std::thread::spawn
    fn assert_send_sync(_x: impl Sync + Send + 'static) {}

    #[test]
    fn test_send_sync_counted_reader() {
        let stream: &[u8] = &[];
        let mut reader = CountedReader::new(stream, |_| {});

        assert_send_sync(move || {
            reader.read_exact(&mut []).unwrap();
        });
    }

    #[test]
    fn test_send_sync_counted_writer() {
        let stream = Vec::<u8>::new();
        let mut writer = CountedWriter::new(stream, |_| {});

        assert_send_sync(move || {
            writer.write_all(&[]).unwrap();
        });
    }
}
