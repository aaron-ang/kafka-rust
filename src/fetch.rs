#![allow(dead_code)]

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::*;

pub struct FetchRequestV16 {
    max_wait_ms: u32,
    min_bytes: u32,
    max_bytes: u32,
    isolation_level: u8,
    pub session_id: u32,
    session_epoch: u32,
    pub topics: Vec<TopicRequest>,
    forgotten_topics_data: Vec<ForgottenTopicData>,
    rack_id: CompactNullableString,
}

impl Deserialize<Self> for FetchRequestV16 {
    fn deserialize(src: &mut Bytes) -> Result<Self> {
        let max_wait_ms = src.get_u32();
        let min_bytes = src.get_u32();
        let max_bytes = src.get_u32();
        let isolation_level = src.get_u8();
        let session_id = src.get_u32();
        let session_epoch = src.get_u32();
        let topics = CompactArray::<Self>::deserialize(src)?;
        let forgotten_topics_data = CompactArray::<Self>::deserialize(src)?;
        let rack_id = CompactNullableString::deserialize(src)?;
        _ = TagBuffer::deserialize(src);

        Ok(Self {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
        })
    }
}

impl Deserialize<TopicRequest> for FetchRequestV16 {
    fn deserialize(src: &mut Bytes) -> Result<TopicRequest> {
        let topic_id = Uuid::deserialize(src)?;
        let partitions = CompactArray::<TopicRequest>::deserialize(src)?;
        _ = TagBuffer::deserialize(src);
        Ok(TopicRequest {
            topic_id,
            partitions,
        })
    }
}

impl Deserialize<ForgottenTopicData> for FetchRequestV16 {
    fn deserialize(src: &mut Bytes) -> Result<ForgottenTopicData> {
        let ftd = ForgottenTopicData {
            topic_id: Uuid::deserialize(src)?,
            partitions: CompactArray::<ForgottenTopicData>::deserialize(src)?,
        };
        _ = TagBuffer::deserialize(src);
        Ok(ftd)
    }
}

pub struct FetchResponseV16 {
    header: HeaderV1,
    throttle_time_ms: i32,
    error_code: ErrorCode,
    session_id: u32,
    responses: CompactArray<TopicResponse>,
}

impl FetchResponseV16 {
    pub fn new(correlation_id: i32, session_id: u32, responses: Vec<TopicResponse>) -> Self {
        let header = HeaderV1::new(correlation_id);

        let resp = Self {
            header,
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            session_id,
            responses: CompactArray(responses),
        };

        resp
    }
}

impl Response for FetchResponseV16 {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::from(self.header.serialize());
        bytes.put_i32(self.throttle_time_ms);
        bytes.put_i16(self.error_code.into());
        bytes.put_u32(self.session_id);
        bytes.put(self.responses.serialize());
        bytes.put(TagBuffer::serialize());
        bytes.freeze()
    }
}

pub fn handle_request(header: HeaderV2, message: &mut Bytes) -> Result<FetchResponseV16> {
    let req: FetchRequestV16 = FetchRequestV16::deserialize(message)?;
    if req.topics.is_empty() {
        let responses = vec![];
        return Ok(FetchResponseV16::new(
            header.correlation_id,
            req.session_id,
            responses,
        ));
    };
    todo!()
}

pub struct TopicRequest {
    pub topic_id: Uuid,
    pub partitions: Vec<Partition>,
}

impl Deserialize<Partition> for TopicRequest {
    fn deserialize(src: &mut Bytes) -> Result<Partition> {
        Ok(Partition {})
    }
}

pub struct TopicResponse {
    topic_id: Uuid,
    partitions: CompactArray<TopicPartition>,
}

impl TopicResponse {
    pub fn new(topic_id: String, partitions: Vec<TopicPartition>) -> Self {
        Self {
            topic_id: Uuid(topic_id),
            partitions: CompactArray(partitions),
        }
    }
}

impl Serialize for TopicResponse {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put(self.topic_id.clone().serialize());
        b.put(self.partitions.serialize());
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

struct ForgottenTopicData {
    topic_id: Uuid,
    partitions: Vec<u32>, // The partitions indexes to forget.
}

impl Deserialize<u32> for ForgottenTopicData {
    fn deserialize(src: &mut Bytes) -> Result<u32> {
        Ok(src.get_u32())
    }
}

pub struct TopicPartition {
    pub partition_index: u32,
    pub error_code: ErrorCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: CompactArray<AbortedTransaction>,
    pub preferred_read_replica: i32,
    pub record_batches: CompactArray<BatchBytes>,
}

impl Serialize for TopicPartition {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32(self.partition_index);
        b.put_i16(self.error_code.into());
        b.put_i64(self.high_watermark);
        b.put_i64(self.last_stable_offset);
        b.put_i64(self.log_start_offset);
        b.put(self.aborted_transactions.serialize());
        b.put_i32(self.preferred_read_replica);
        b.put(self.record_batches.serialize());
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

pub struct AbortedTransaction {
    producer_id: u64,
    first_offset: u64,
}

impl Serialize for AbortedTransaction {
    fn serialize(&self) -> Bytes {
        todo!()
    }
}

pub struct BatchBytes {
    pub bytes: Bytes,
}

impl Serialize for BatchBytes {
    fn serialize(&self) -> Bytes {
        self.bytes.clone()
    }
}

pub struct Partition {}
