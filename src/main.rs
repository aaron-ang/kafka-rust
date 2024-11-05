mod api_versions;
mod protocol;

use api_versions::ApiVersionsResponseV3;
use protocol::*;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").await?;

    loop {
        let (stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            println!("accepted new connection");
            if let Err(e) = handle_conn(stream).await {
                eprintln!("error handling request: {:?}", e);
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream) -> Result<()> {
    loop {
        let mut message = get_msg(&mut stream).await?;
        let resp = process_request(&mut message)?;
        let resp_msg = ResponseMessage::new(resp.as_bytes());
        stream.write(resp_msg.as_bytes()).await?;
    }
}

async fn get_msg(stream: &mut TcpStream) -> Result<Bytes> {
    let mut len_buf = [0; 4];
    stream.read_exact(&mut len_buf).await?;

    let msg_len = i32::from_be_bytes(len_buf) as usize;
    let mut msg_buf = vec![0; msg_len];
    stream.read_exact(&mut msg_buf).await?;

    Ok(Bytes::from(msg_buf))
}

fn process_request(message: &mut Bytes) -> Result<Box<dyn Response + Send>> {
    let header = HeaderV2::deserialize(message)?;
    let request_api_key = match ApiKey::try_from(header.api_key) {
        Ok(key) => key,
        Err(_) => {
            return Err(anyhow!("Invalid request api key"));
        }
    };

    let response = match request_api_key {
        ApiKey::Fetch => todo!(),
        ApiKey::ApiVersions => {
            let resp = ApiVersionsResponseV3::new(header);
            Box::new(resp)
        }
        ApiKey::DescribeTopicPartitions => todo!(),
    };

    Ok(response)
}
