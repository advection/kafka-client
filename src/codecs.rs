use std::default::Default;
use std::io::{Read, Write, self};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::error::{KafkaErrorKind, KafkaError};


fn convert_to_i16(t:usize) -> Result<i16, KafkaError> {
    let x: usize = t;
    if (x as u64) <= (i16::max_value() as u64) {
        Ok(x as i16)
    } else {
        Err(KafkaErrorKind::CodecError.into())
    }
}

fn convert_to_i32(t:usize) -> Result<i32, KafkaError> {
    let x: usize = t;
    if (x as u64) <= (i32::max_value() as u64) {
        Ok(x as i32)
    } else {
        Err(KafkaErrorKind::CodecError.into())
    }
}


pub trait ToByte {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<(), KafkaError>;
}

impl<'a, T: ToByte + 'a + ?Sized> ToByte for &'a T {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<(), KafkaError> {
        (*self).encode(buffer)
    }
}

impl ToByte for i8 {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<(), KafkaError> {
        buffer.write_i8(*self)
            .or_else(|e| Err(KafkaErrorKind::IoError(e).into()))
    }
}

impl ToByte for i16 {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<(), KafkaError> {
        buffer
            .write_i16::<BigEndian>(*self)
            .map_err(|e| KafkaErrorKind::IoError(e).into())
    }
}

impl ToByte for i32 {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<(), KafkaError> {
        buffer
            .write_i32::<BigEndian>(*self)
            .or_else(|e| Err(KafkaErrorKind::IoError(e).into()))
    }
}

impl ToByte for i64 {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<(), KafkaError> {
        buffer
            .write_i64::<BigEndian>(*self)
            .or_else(|e| Err(KafkaErrorKind::IoError(e).into()))
    }
}

impl ToByte for str {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<(), KafkaError> {
        let l = convert_to_i16(self.len())?;
        buffer.write_i16::<BigEndian>(l)?;
        buffer
            .write_all(self.as_bytes())
            .or_else(|e| Err(KafkaErrorKind::IoError(e).into()))
    }
}

#[test]
fn test_string_too_long() {
    use std::str;

    let s = vec![b'a'; i16::max_value() as usize + 1];
    let s = unsafe { str::from_utf8_unchecked(&s) };
    let mut buf = Vec::new();
    assert!(s.encode(&mut buf).is_err());
    assert!(buf.is_empty());
}

impl<V: ToByte> ToByte for [V] {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<(), KafkaError> {
        encode_as_array(buffer, self, |buffer, x| x.encode(buffer))
    }
}

impl ToByte for [u8] {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<(), KafkaError> {
        let l = convert_to_i32(self.len())?;
        buffer.write_i32::<BigEndian>(l)?;
        buffer
            .write_all(self)
            .map_err(|e| KafkaErrorKind::IoError(e).into())
    }
}

// ~ this allows to render a slice of various types (typically &str
// and String) as strings
pub struct AsStrings<'a, T: 'a>(pub &'a [T]);

impl<'a, T: AsRef<str> + 'a> ToByte for AsStrings<'a, T> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<(), KafkaError> {
        encode_as_array(buffer, self.0, |buffer, x| x.as_ref().encode(buffer))
    }
}

/// ~ Renders the length of `xs` to `buffer` as the start of a
/// protocol array and then for each element of `xs` invokes `f`
/// assuming that function will render the element to the buffer.
pub fn encode_as_array<T, F, W>(buffer: &mut W, xs: &[T], mut f: F) -> Result<(), KafkaError>
where
    F: FnMut(&mut W, &T) -> Result<(), KafkaError>,
    W: Write,
{
    let l = convert_to_i32(xs.len())?;
    buffer.write_i32::<BigEndian>(l).map_err(|e| KafkaErrorKind::IoError(e))?;
    for x in xs {
        f(buffer, x)?;
    }
    Ok(())
}

// --------------------------------------------------------------------

pub trait FromByte {
    type R: Default + FromByte;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<(), KafkaError>;
    fn decode_new<T: Read>(buffer: &mut T) -> Result<Self::R, KafkaError> {
        let mut temp: Self::R = Default::default();
        match temp.decode(buffer) {
            Ok(_) => Ok(temp),
            Err(e) => Err(e),
        }
    }
}

macro_rules! dec_helper {
    ($val: expr, $dest: expr) => {{
        match $val {
            Ok(val) => {
                $dest = val;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }};
}
macro_rules! decode {
    ($src:expr, $dest: expr) => {{
        dec_helper!($src.read_i8(), $dest)
    }};
    ($src:expr, $method:ident, $dest: expr) => {{
        dec_helper!($src.$method::<BigEndian>(), $dest)
    }};
}

impl FromByte for i8 {
    type R = i8;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<(), KafkaError> {
        decode!(buffer, *self)
    }
}

impl FromByte for i16 {
    type R = i16;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<(), KafkaError> {
        decode!(buffer, read_i16, *self)
    }
}

impl FromByte for i32 {
    type R = i32;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<(), KafkaError> {
        decode!(buffer, read_i32, *self)
    }
}

impl FromByte for i64 {
    type R = i64;
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<(), KafkaError> {
        decode!(buffer, read_i64, *self)
    }
}

impl FromByte for String {
    type R = String;
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<(), KafkaError> {
        let mut length: i16 = 0;
        if let Err(e) = decode!(buffer, read_i16, length) {
            return Err(e);
        }
        if length <= 0 {
            return Ok(());
        }
        self.reserve(length as usize);
        let _ = buffer.take(length as u64).read_to_string(self).map_err(|e| KafkaErrorKind::IoError(e))?;
        if self.len() != length as usize {
            Err(KafkaErrorKind::IoError(io::ErrorKind::UnexpectedEof.into()).into()) // zlb: I get it, lots of intos
        } else { Ok(()) }
    }
}

impl<V: FromByte + Default> FromByte for Vec<V> {
    type R = Vec<V>;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<(), KafkaError> {
        let mut length: i32 = 0;
        if let Err(e) = decode!(buffer, read_i32, length) {
            return Err(e);
        }
        if length <= 0 {
            return Ok(());
        }
        self.reserve(length as usize);
        for _ in 0..length {
            let mut e: V = Default::default();
            e.decode(buffer)?;
            self.push(e);
        }
        Ok(())
    }
}

impl FromByte for Vec<u8> {
    type R = Vec<u8>;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<(), KafkaError> {
        let mut length: i32 = 0;
        match decode!(buffer, read_i32, length) {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        if length <= 0 {
            return Ok(());
        }
        self.reserve(length as usize);
        let size = buffer.take(length as u64)
            .read_to_end(self)
            .map_err(|e| KafkaError::from(e))?;

        if size < length as usize {
            Err(KafkaError::from(io::Error::new(io::ErrorKind::UnexpectedEof, "size was shorter than expected")))?
        } else {
            Ok(())
        }
    }
}

#[test]
fn codec_i8() {
    use std::io::Cursor;
    let mut buf = vec![];
    let orig: i8 = 5;

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [5]);

    // Read from buffer into existing variable
    let mut dec1: i8 = 0;
    dec1.decode(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = i8::decode_new(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_i16() {
    use std::io::Cursor;
    let mut buf = vec![];
    let orig: i16 = 5;

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 5]);

    // Read from buffer into existing variable
    let mut dec1: i16 = 0;
    dec1.decode(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = i16::decode_new(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_32() {
    use std::io::Cursor;
    let mut buf = vec![];
    let orig: i32 = 5;

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 5]);

    // Read from buffer into existing variable
    let mut dec1: i32 = 0;
    dec1.decode(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = i32::decode_new(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_i64() {
    use std::io::Cursor;
    let mut buf = vec![];
    let orig: i64 = 5;

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 0, 0, 0, 0, 5]);

    // Read from buffer into existing variable
    let mut dec1: i64 = 0;
    dec1.decode(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = i64::decode_new(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_string() {
    use std::io::Cursor;
    let mut buf = vec![];
    let orig = "test".to_owned();

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 4, 116, 101, 115, 116]);

    // Read from buffer into existing variable
    let mut dec1 = String::new();
    dec1.decode(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = String::decode_new(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_vec_u8() {
    use std::io::Cursor;
    let mut buf = vec![];
    let orig: Vec<u8> = vec![1, 2, 3];

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 3, 1, 2, 3]);

    // Read from buffer into existing variable
    let mut dec1: Vec<u8> = vec![];
    dec1.decode(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = Vec::<u8>::decode_new(&mut Cursor::new(&buf[..])).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_as_strings() {
    macro_rules! enc_dec_cmp {
        ($orig:expr) => {{
            use std::io::Cursor;

            let orig = $orig;

            // Encode into buffer
            let mut buf = Vec::new();
            AsStrings(&orig).encode(&mut buf).unwrap();
            assert_eq!(
                buf,
                [0, 0, 0, 2, 0, 3, b'a', b'b', b'c', 0, 4, b'd', b'e', b'f', b'g']
            );

            // Decode from buffer into existing value
            {
                let mut dec: Vec<String> = Vec::new();
                dec.decode(&mut Cursor::new(&buf[..])).unwrap();
                assert_eq!(dec, orig);
            }

            // Read from buffer into new variable
            {
                let dec = Vec::<String>::decode_new(&mut Cursor::new(&buf[..])).unwrap();
                assert_eq!(dec, orig);
            }
        }};
    }

    {
        // slice of &str
        let orig: &[&str] = &["abc", "defg"];
        enc_dec_cmp!(orig);
    }

    {
        // vec of &str
        let orig: Vec<&str> = vec!["abc", "defg"];
        enc_dec_cmp!(orig);
    }

    {
        // vec of String
        let orig: Vec<String> = vec!["abc".to_owned(), "defg".to_owned()];
        enc_dec_cmp!(orig);
    }
}
