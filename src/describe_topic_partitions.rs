use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::*;

const DEFAULT_UNKNOWN_TOPIC_UUID: &str = "00000000-0000-0000-0000-000000000000";

pub fn handle_request(
    header: HeaderV2,
    message: &mut Bytes,
) -> Result<DescribeTopicPartitionsResponseV0> {
    let req = DescribeTopicPartitionsRequestV0::deserialize(message)?;
    let topic_id = DEFAULT_UNKNOWN_TOPIC_UUID.to_string();
    let topic_error_code = ErrorCode::UnknownTopicOrPartition;
    let mut topics = vec![];

    for topic_name in req.topic_names {
        let topic = Topic {
            error_code: topic_error_code,
            name: topic_name,
            topic_id: topic_id.clone(),
            is_internal: false,
            partitions: CompactArray(vec![]),
            topic_authorized_operations: 0,
        };
        topics.push(topic);
    }

    Ok(DescribeTopicPartitionsResponseV0::new(
        header.correlation_id,
        topics,
    ))
}

pub struct DescribeTopicPartitionsRequestV0 {
    pub topic_names: Vec<String>,
    response_partition_limit: i32,
    cursor: u8,
}

impl DescribeTopicPartitionsRequestV0 {
    pub fn deserialize(src: &mut Bytes) -> Result<Self> {
        let topic_names = CompactArray::<Topic>::deserialize(src)?;
        let response_partition_limit = src.get_i32();
        let cursor = src.get_u8();
        _ = TagBuffer::deserialize(src);

        Ok(Self {
            topic_names,
            response_partition_limit,
            cursor,
        })
    }
}

pub struct DescribeTopicPartitionsResponseV0 {
    header: HeaderV1,
    throttle_time_ms: i32,
    topics: CompactArray<Topic>,
    next_cursor: u8,
}

impl DescribeTopicPartitionsResponseV0 {
    pub fn new(correlation_id: i32, topics: Vec<Topic>) -> Self {
        let header = HeaderV1::new(correlation_id);
        let resp = Self {
            header,
            throttle_time_ms: 0,
            topics: CompactArray(topics),
            next_cursor: 0xFF,
        };
        resp
    }
}

impl Response for DescribeTopicPartitionsResponseV0 {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::from(self.header.serialize());
        bytes.put_i32(self.throttle_time_ms);
        bytes.put(self.topics.serialize());
        bytes.put_u8(self.next_cursor);
        bytes.put(TagBuffer::serialize());
        bytes.freeze()
    }
}

pub struct Topic {
    pub error_code: ErrorCode,
    pub name: String,
    pub topic_id: String,
    pub is_internal: bool,
    pub partitions: CompactArray<Partition>,
    pub topic_authorized_operations: i32,
}

impl Serialize for Topic {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put(self.error_code.serialize());
        b.put(CompactString::serialize(&self.name));
        b.put(Uuid::serialize(&self.topic_id));
        b.put_u8(self.is_internal.into());
        b.put(self.partitions.serialize());
        b.put_i32(self.topic_authorized_operations);
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

impl Deserialize<String> for Topic {
    fn deserialize(src: &mut Bytes) -> Result<String> {
        let s = CompactString::deserialize(src);
        _ = TagBuffer::deserialize(src);
        s
    }
}

pub struct Partition {}

impl Serialize for Partition {
    fn serialize(&self) -> Bytes {
        let b = BytesMut::new();
        b.freeze()
    }
}
