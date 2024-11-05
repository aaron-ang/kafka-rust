use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::*;
use num_enum::{IntoPrimitive, TryFromPrimitive};

pub trait Response {
    fn as_bytes(&self) -> &[u8];
}

pub trait Serialize {
    fn serialize(&mut self) -> Bytes;
}

pub trait Deserialize<T> {
    fn deserialize(src: &mut Bytes) -> Result<T>;
}

#[derive(Debug, Clone, Copy, IntoPrimitive, TryFromPrimitive)]
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
    UnsupportedVersion = 35,
}

impl Serialize for ErrorCode {
    fn serialize(&mut self) -> Bytes {
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

    pub fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_i32(self.correlation_id);
        bytes.freeze()
    }
}

pub struct HeaderV2 {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

impl HeaderV2 {
    pub fn deserialize(src: &mut Bytes) -> Result<Self> {
        let api_key = src.get_i16();
        let api_version = src.get_i16();
        let correlation_id = src.get_i32();
        let client_id = NullableString::deserialize(src)?;

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

pub struct ResponseMessage {
    bytes: BytesMut,
}

impl ResponseMessage {
    pub fn new(src: &[u8]) -> Self {
        let mut bytes = BytesMut::with_capacity(src.len() + 4);
        let msg_size = src.len() as i32;

        bytes.put_i32(msg_size);
        bytes.put_slice(src);
        Self { bytes }
    }
}

impl Response for ResponseMessage {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub struct NullableString;

impl NullableString {
    pub fn deserialize(src: &mut Bytes) -> Result<String> {
        let len = src.get_i16();
        let string_len = if len == -1 { 0 } else { len as usize };
        if src.remaining() < string_len {
            return Err(anyhow!("Not enough bytes to read string"));
        }
        let bytes = src.split_to(string_len);
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

pub struct CompactArray;

impl CompactArray {
    pub fn serialize<T: Serialize>(items: &mut [T]) -> Bytes {
        let mut b = BytesMut::new();
        let len = (items.len() + 1) as u8;
        b.put_u8(len);

        for item in items.iter_mut() {
            b.put(item.serialize());
        }

        b.freeze()
    }

    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Result<Vec<T>> {
        let (len, _) = i32::decode_var(src).unwrap();
        let items_len = if len > 1 { len as usize - 1 } else { 0 };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            let item = U::deserialize(src)?;
            items.push(item);
        }

        Ok(items)
    }
}

pub struct TaggedFields;

impl TaggedFields {
    pub fn serialize() -> Bytes {
        let mut b = BytesMut::with_capacity(1);
        b.put_u8(0);
        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> u8 {
        src.get_u8()
    }
}
