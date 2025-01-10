//! Frontend message serialization.
#![allow(missing_docs)]

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, BytesMut};
use std::error::Error;
use std::io;
use std::marker;

use crate::{write_nullable, FromUsize, IsNull, Oid};

#[inline]
fn write_body<F, E>(buf: &mut BytesMut, f: F) -> Result<(), E>
where
    F: FnOnce(&mut BytesMut) -> Result<(), E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    f(buf)?;

    let size = i32::from_usize(buf.len() - base)?;
    BigEndian::write_i32(&mut buf[base..], size);
    Ok(())
}

pub enum BindError {
    Conversion(Box<dyn Error + marker::Sync + Send>),
    Serialization(io::Error),
}

impl From<Box<dyn Error + marker::Sync + Send>> for BindError {
    #[inline]
    fn from(e: Box<dyn Error + marker::Sync + Send>) -> BindError {
        BindError::Conversion(e)
    }
}

impl From<io::Error> for BindError {
    #[inline]
    fn from(e: io::Error) -> BindError {
        BindError::Serialization(e)
    }
}

#[inline]
pub fn bind<I, J, F, T, K>(
    portal: &str,
    statement: &str,
    formats: I,
    values: J,
    mut serializer: F,
    result_formats: K,
    buf: &mut BytesMut,
) -> Result<(), BindError>
where
    I: IntoIterator<Item = i16>,
    J: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesMut) -> Result<IsNull, Box<dyn Error + marker::Sync + Send>>,
    K: IntoIterator<Item = i16>,
{
    buf.put_u8(b'B');

    write_body(buf, |buf| {
        write_cstr(portal.as_bytes(), buf)?;
        write_cstr(statement.as_bytes(), buf)?;
        write_counted(
            formats,
            |f, buf| {
                buf.put_i16(f);
                Ok::<_, io::Error>(())
            },
            buf,
        )?;
        write_counted(
            values,
            |v, buf| write_nullable(|buf| serializer(v, buf), buf),
            buf,
        )?;
        write_counted(
            result_formats,
            |f, buf| {
                buf.put_i16(f);
                Ok::<_, io::Error>(())
            },
            buf,
        )?;

        Ok(())
    })
}

#[inline]
fn write_counted<I, T, F, E>(items: I, mut serializer: F, buf: &mut BytesMut) -> Result<(), E>
where
    I: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesMut) -> Result<(), E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 2]);
    let mut count = 0;
    for item in items {
        serializer(item, buf)?;
        count += 1;
    }
    let count = i16::from_usize(count)?;
    BigEndian::write_i16(&mut buf[base..], count);

    Ok(())
}

#[inline]
pub fn cancel_request(process_id: i32, secret_key: i32, buf: &mut BytesMut) {
    write_body(buf, |buf| {
        buf.put_i32(80_877_102);
        buf.put_i32(process_id);
        buf.put_i32(secret_key);
        Ok::<_, io::Error>(())
    })
    .unwrap();
}

#[inline]
pub fn close(variant: u8, name: &str, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'C');
    write_body(buf, |buf| {
        buf.put_u8(variant);
        write_cstr(name.as_bytes(), buf)
    })
}

pub struct CopyData<T> {
    buf: T,
    len: i32,
}

impl<T> CopyData<T>
where
    T: Buf,
{
    pub fn new(buf: T) -> io::Result<CopyData<T>> {
        let len = buf
            .remaining()
            .checked_add(4)
            .and_then(|l| i32::try_from(l).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "message length overflow")
            })?;

        Ok(CopyData { buf, len })
    }

    pub fn write(self, out: &mut BytesMut) {
        out.put_u8(b'd');
        out.put_i32(self.len);
        out.put(self.buf);
    }
}

#[inline]
pub fn copy_done(buf: &mut BytesMut) {
    buf.put_u8(b'c');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
pub fn copy_fail(message: &str, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'f');
    write_body(buf, |buf| write_cstr(message.as_bytes(), buf))
}

#[inline]
pub fn describe(variant: u8, name: &str, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'D');
    write_body(buf, |buf| {
        buf.put_u8(variant);
        write_cstr(name.as_bytes(), buf)
    })
}

#[inline]
pub fn execute(portal: &str, max_rows: i32, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'E');
    write_body(buf, |buf| {
        write_cstr(portal.as_bytes(), buf)?;
        buf.put_i32(max_rows);
        Ok(())
    })
}

#[inline]
pub fn parse<I>(name: &str, query: &str, param_types: I, buf: &mut BytesMut) -> io::Result<()>
where
    I: IntoIterator<Item = Oid>,
{
    buf.put_u8(b'P');
    write_body(buf, |buf| {
        write_cstr(name.as_bytes(), buf)?;
        write_cstr(query.as_bytes(), buf)?;
        write_counted(
            param_types,
            |t, buf| {
                buf.put_u32(t);
                Ok::<_, io::Error>(())
            },
            buf,
        )?;
        Ok(())
    })
}

#[inline]
pub fn password_message(password: &[u8], buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'p');
    write_body(buf, |buf| write_cstr(password, buf))
}

#[inline]
pub fn query(query: &str, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'Q');
    write_body(buf, |buf| write_cstr(query.as_bytes(), buf))
}

#[inline]
pub fn sasl_initial_response(mechanism: &str, data: &[u8], buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'p');
    write_body(buf, |buf| {
        write_cstr(mechanism.as_bytes(), buf)?;
        let len = i32::from_usize(data.len())?;
        buf.put_i32(len);
        buf.put_slice(data);
        Ok(())
    })
}

#[inline]
pub fn sasl_response(data: &[u8], buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u8(b'p');
    write_body(buf, |buf| {
        buf.put_slice(data);
        Ok(())
    })
}

#[inline]
pub fn ssl_request(buf: &mut BytesMut) {
    write_body(buf, |buf| {
        buf.put_i32(80_877_103);
        Ok::<_, io::Error>(())
    })
    .unwrap();
}

#[inline]
pub fn startup_message(parameters: &StartupMessageParams, buf: &mut BytesMut) -> io::Result<()> {
    write_body(buf, |buf| {
        // postgres protocol version 3.0(196608) in bigger-endian
        buf.put_i32(0x00_03_00_00);
        buf.put_slice(&parameters.params);
        buf.put_u8(0);
        Ok(())
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupMessageParams {
    pub params: BytesMut,
}

impl StartupMessageParams {
    /// Set parameter's value by its name.
    pub fn insert(&mut self, name: &str, value: &str) {
        if name.contains('\0') || value.contains('\0') {
            panic!("startup parameter name or value contained a null")
        }
        self.params.put_slice(name.as_bytes());
        self.params.put_u8(0);
        self.params.put_slice(value.as_bytes());
        self.params.put_u8(0);
    }
}

#[inline]
pub fn sync(buf: &mut BytesMut) {
    buf.put_u8(b'S');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
pub fn terminate(buf: &mut BytesMut) {
    buf.put_u8(b'X');
    write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
}

#[inline]
fn write_cstr(s: &[u8], buf: &mut BytesMut) -> Result<(), io::Error> {
    if s.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "string contains embedded null",
        ));
    }
    buf.put_slice(s);
    buf.put_u8(0);
    Ok(())
}
