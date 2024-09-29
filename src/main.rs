use bytes::{Buf, BufMut};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

struct RequestHeader {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
}

fn handle_request(mut stream: TcpStream) {
    let header = parse_header(&stream);
    let response = create_response(header);
    stream.write(&response).unwrap();
}

fn parse_header(mut stream: &TcpStream) -> RequestHeader {
    let mut buf = [0; 4];
    stream.read_exact(&mut buf).unwrap();
    let len = u32::from_be_bytes(buf) as usize;

    let mut msg = vec![0; len];
    stream.read_exact(&mut msg).unwrap();

    let mut msg = msg.as_slice();
    let api_key = msg.get_i16();
    let api_version = msg.get_i16();
    let correlation_id = msg.get_i32();

    RequestHeader {
        api_key,
        api_version,
        correlation_id,
    }
}

fn create_response(header: RequestHeader) -> Vec<u8> {
    let mut data = Vec::new();
    data.put_i32(header.correlation_id);
    if header.api_key == 18 {
        let mut error_code: i16 = 0;
        if !(0..=4).contains(&header.api_version) {
            error_code = 35;
        }
        data.put_i16(error_code); // error_code
        data.put_i8(2); // api_keys
        data.put_i16(header.api_key);
        data.put_i16(0); // min_version
        data.put_i16(4); // max_version
        data.put_i8(0); // _tagged_fields
        data.put_i32(0); // throttle_time_ms
        data.put_i8(0); // _tagged_fields
    }

    let mut response = Vec::new();
    response.put_i32(data.len().try_into().unwrap());
    response.put(data.as_slice());
    response
}

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_request(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
