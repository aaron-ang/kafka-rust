use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::*;
use num_enum::{IntoPrimitive, TryFromPrimitive};

pub trait Response {
    fn as_bytes(&self) -> Bytes;
}

pub trait Serialize {
    fn serialize(&self) -> Bytes;
}

pub trait Deserialize<T> {
    fn deserialize(src: &mut Bytes) -> Result<T>;
}

#[derive(Clone, Copy, IntoPrimitive, TryFromPrimitive)]
#[repr(i16)]
pub enum ApiKey {
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

#[derive(Clone, Copy, IntoPrimitive)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
}

impl Serialize for ErrorCode {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(2);
        let val = (*self).into();
        b.put_i16(val);
        b.freeze()
    }
}

pub struct HeaderV0 {
    correlation_id: i32,
}

impl HeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl Serialize for HeaderV0 {
    fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_i32(self.correlation_id);
        bytes.freeze()
    }
}

pub struct HeaderV1 {
    correlation_id: i32,
}

impl HeaderV1 {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl Serialize for HeaderV1 {
    fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(5);
        bytes.put_i32(self.correlation_id);
        bytes.put(TagBuffer::serialize());
        bytes.freeze()
    }
}

pub struct HeaderV2 {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

impl Deserialize<Self> for HeaderV2 {
    fn deserialize(src: &mut Bytes) -> Result<Self> {
        let api_key = src.get_i16();
        let api_version = src.get_i16();
        let correlation_id = src.get_i32();
        let client_id = NullableString::deserialize(src)?;
        _ = TagBuffer::deserialize(src);

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

pub struct Uuid;

impl Uuid {
    pub fn serialize(s: &str) -> Bytes {
        let mut b = BytesMut::with_capacity(32);
        b.extend_from_slice(&hex::decode(s.replace('-', "")).expect("valid UUID string"));
        b.freeze()
    }
}

impl Deserialize<String> for Uuid {
    fn deserialize(src: &mut Bytes) -> Result<String> {
        let mut s = hex::encode(src.slice(..16));
        src.advance(16);
        s.insert(8, '-');
        s.insert(13, '-');
        s.insert(18, '-');
        s.insert(23, '-');
        Ok(s)
    }
}

pub struct CompactString;

impl CompactString {
    pub fn serialize(s: &str) -> Bytes {
        let mut b = BytesMut::new();
        let len = s.len() as u8 + 1;
        b.put_u8(len);
        b.put(s.as_bytes());
        b.freeze()
    }
}

impl Deserialize<String> for CompactString {
    fn deserialize(src: &mut Bytes) -> Result<String> {
        let (len, read) = u32::decode_var(src).ok_or_else(|| anyhow!("Failed to decode length"))?;
        src.advance(read);
        let string_len = if len > 1 { len as usize - 1 } else { 0 };
        let bytes = src.slice(..string_len);
        src.advance(string_len);
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

pub struct NullableString;

impl Deserialize<String> for NullableString {
    fn deserialize(src: &mut Bytes) -> Result<String> {
        let len = src.get_i16();
        let string_len = if len == -1 { 0 } else { len as usize };
        if src.remaining() < string_len {
            return Err(anyhow!("Not enough bytes to read string"));
        }
        let bytes = src.split_to(string_len);
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

pub struct CompactArray<T>(pub Vec<T>);

impl<T: Serialize> Serialize for CompactArray<T> {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        let len = self.0.len() as u8 + 1;
        b.put_u8(len);
        for item in &self.0 {
            b.put(item.serialize());
        }
        b.freeze()
    }
}

impl<T, U> Deserialize<Vec<U>> for CompactArray<T>
where
    T: Deserialize<U>, // T must know how to deserialize into U
{
    fn deserialize(src: &mut Bytes) -> Result<Vec<U>> {
        let (len, read) = u32::decode_var(src).ok_or_else(|| anyhow!("Failed to decode length"))?;
        src.advance(read);
        let items_len = if len > 1 { len as usize - 1 } else { 0 };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            let item = T::deserialize(src)?;
            items.push(item);
        }
        Ok(items)
    }
}

pub struct TagBuffer;

impl TagBuffer {
    pub fn serialize() -> Bytes {
        let mut b = BytesMut::with_capacity(1);
        b.put_u8(0);
        b.freeze()
    }
}

impl Deserialize<u8> for TagBuffer {
    fn deserialize(src: &mut Bytes) -> Result<u8> {
        Ok(src.get_u8())
    }
}
