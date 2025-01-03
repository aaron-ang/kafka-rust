use std::path::Path;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::*;
use num_enum::TryFromPrimitive;

use crate::protocol::*;

pub struct RecordBatches {
    batches: Vec<RecordBatch>,
}

impl RecordBatches {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let file_bytes = std::fs::read(path)?;
        let mut data = Bytes::from(file_bytes);
        let mut batches = vec![];
        while data.has_remaining() {
            let record_batch = RecordBatch::from_bytes(&mut data)?;
            batches.push(record_batch);
        }
        Ok(Self { batches })
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn raw_batch_for_topic(&self, topic_id: Uuid, partition_id: u32) -> Result<Option<Bytes>> {
        let mut topic_name = String::new();

        self.batches.iter().find(|&b| {
            b.records.iter().any(|r| match &r.value {
                RecordValue::Topic(topic) if topic.topic_id == topic_id => {
                    topic_name = topic.topic_name.clone().0.unwrap_or_default();
                    true
                }
                _ => false,
            })
        });

        if topic_name.is_empty() {
            return Ok(None);
        }

        let file = format!(
            "/tmp/kraft-combined-logs/{}-{}/00000000000000000000.log",
            topic_name, partition_id
        );
        let file_bytes = std::fs::read(file)?;
        Ok(Some(Bytes::from(file_bytes)))
    }
}

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: u32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    pub records: Vec<Record>,
}

impl RecordBatch {
    pub fn from_bytes(src: &mut Bytes) -> Result<Self> {
        let base_offset = src.get_i64();
        let batch_length = src.get_i32();
        let partition_leader_epoch = src.get_i32();
        let magic = src.get_i8();
        let crc = src.get_u32();
        let attributes = src.get_i16();
        let last_offset_delta = src.get_i32();
        let base_timestamp = src.get_i64();
        let max_timestamp = src.get_i64();
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let base_sequence = src.get_i32();
        let records = NullableBytes::<RecordBatch>::deserialize(src);

        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

impl Deserialize<Record> for RecordBatch {
    fn deserialize(src: &mut Bytes) -> Record {
        Record::from_bytes(src)
    }
}

impl Serialize for RecordBatch {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i64(self.base_offset);
        b.put_i32(self.batch_length);
        b.put_i32(self.partition_leader_epoch);
        b.put_i8(self.magic);
        b.put_u32(self.crc);
        b.put_i16(self.attributes);
        b.put_i32(self.last_offset_delta);
        b.put_i64(self.base_timestamp);
        b.put_i64(self.max_timestamp);
        b.put_i64(self.producer_id);
        b.put_i16(self.producer_epoch);
        b.put_i32(self.base_sequence);
        b.freeze()
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    length: i64,
    attributes: i8,
    timestamp_delta: i64,
    offset_delta: i64,
    key: Vec<u8>,
    value_length: i64,
    pub value: RecordValue,
    headers: Vec<Header>,
}

impl Record {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let (length, read) = i64::decode_var(src).expect("Failed to decode length");
        src.advance(read);

        let attributes = src.get_i8();

        let (timestamp_delta, read) =
            i64::decode_var(src).expect("Failed to decode timestamp delta");
        src.advance(read);

        let (offset_delta, read) = i64::decode_var(src).expect("Failed to decode offset delta");
        src.advance(read);

        let (key_len, read) = i64::decode_var(src).expect("Failed to decode length");
        src.advance(read);

        let key = if key_len > 0 {
            src.split_to(key_len as usize).to_vec()
        } else {
            vec![]
        };

        let (value_length, read) = i64::decode_var(src).expect("Failed to decode value length");
        src.advance(read);

        let value = RecordValue::from_bytes(src);

        let headers = CompactArray::<Record>::deserialize(src);

        Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value_length,
            value,
            headers,
        }
    }
}

impl Deserialize<Header> for Record {
    fn deserialize(_src: &mut Bytes) -> Header {
        Header
    }
}

#[derive(Debug, Clone, Copy)]
struct Header;

#[derive(Debug, Clone)]
pub enum RecordValue {
    FeatureLevel(FeatureLevelValue),
    Topic(TopicValue),
    Partition(PartitionValue),
}

#[derive(Debug, Clone)]
pub struct TopicValue {
    pub topic_name: CompactNullableString,
    pub topic_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct PartitionValue {
    pub partition_id: u32,
    pub topic_id: Uuid,
    pub replicas: Vec<u32>,
    pub in_sync_replicas: Vec<u32>,
    pub removing_replicas: Vec<u32>,
    pub adding_replicas: Vec<u32>,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub partition_epoch: u32,
    pub directories: Vec<Uuid>,
}

impl Deserialize<u32> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> u32 {
        src.get_u32()
    }
}

impl Deserialize<Uuid> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> Uuid {
        Uuid::deserialize(src)
    }
}

#[derive(Debug, Clone)]
pub struct FeatureLevelValue {
    name: CompactNullableString,
    level: u16,
}

#[derive(TryFromPrimitive)]
#[repr(u8)]
enum RecordType {
    Topic = 2,
    Partition,
    FeatureLevel = 12,
}

impl RecordValue {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let frame_version = src.get_u8();
        assert_eq!(frame_version, 1);
        let record_type = RecordType::try_from(src.get_u8()).unwrap();
        let version = src.get_u8();

        let record_value = match record_type {
            RecordType::Topic => {
                assert_eq!(version, 0);
                let topic_name = CompactNullableString::deserialize(src);
                let topic_id = Uuid::deserialize(src);

                RecordValue::Topic(TopicValue {
                    topic_name,
                    topic_id,
                })
            }
            RecordType::Partition => {
                assert_eq!(version, 1);
                let partition_id = src.get_u32();
                let topic_id = Uuid::deserialize(src);

                let replicas = CompactArray::<PartitionValue>::deserialize(src);
                let in_sync_replicas = CompactArray::<PartitionValue>::deserialize(src);
                let removing_replicas = CompactArray::<PartitionValue>::deserialize(src);
                let adding_replicas = CompactArray::<PartitionValue>::deserialize(src);

                let leader_id = src.get_u32();
                let leader_epoch = src.get_u32();
                let partition_epoch = src.get_u32();
                let directories = CompactArray::<PartitionValue>::deserialize(src);

                RecordValue::Partition(PartitionValue {
                    partition_id,
                    topic_id,
                    replicas,
                    in_sync_replicas,
                    removing_replicas,
                    adding_replicas,
                    leader_id,
                    leader_epoch,
                    partition_epoch,
                    directories,
                })
            }
            RecordType::FeatureLevel => {
                assert_eq!(version, 0);
                let name = CompactNullableString::deserialize(src);
                let level = src.get_u16();
                RecordValue::FeatureLevel(FeatureLevelValue { name, level })
            }
        };

        let (tagged_fields_count, read) =
            i64::decode_var(src).expect("Failed to decode tagged fields count");
        src.advance(read);
        assert_eq!(tagged_fields_count, 0);

        record_value
    }
}
