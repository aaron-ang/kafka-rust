use bytes::{Buf, BufMut};
use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0; 4];
                stream.read_exact(&mut buf).unwrap();
                let len = u32::from_be_bytes(buf) as usize;

                let mut msg = vec![0; len];
                stream.read_exact(&mut msg).unwrap();

                let mut msg = msg.as_slice();
                let _api_key = msg.get_i16();
                let api_version = msg.get_i16();
                let correlation_id = msg.get_i32();

                let mut response = Vec::new();
                response.put_i32(0); // todo: message length
                response.put_i32(correlation_id);
                if api_version < 0 || api_version > 4 {
                    response.put_i16(35);
                }
                stream.write(&response).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
