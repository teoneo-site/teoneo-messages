use std::time::Duration;

use futures_lite::stream::StreamExt;
use lapin::{
    Connection, ConnectionProperties, Error, message::Delivery, options::*, types::FieldTable,
};
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, SmtpTransport, Tokio1Executor, Transport,
    message::header::ContentType, transport::smtp::authentication::Credentials,
};
use serde::{Deserialize, Serialize};
use tokio::{runtime::Runtime, task::JoinSet};
use tracing::info;

#[derive(Deserialize, Serialize)]
pub struct MessageJSON {
    email: String, // Кому отправить сообщение
    subject: String,
    message: String, // Полный текст сообщения, который нужно отправить
}

pub async fn handle_delivery(
    smtp: &AsyncSmtpTransport<Tokio1Executor>,
    delivery: &Delivery,
) -> anyhow::Result<()> {
    let del_msg = String::from_utf8_lossy(&delivery.data);
    let msg_struct: MessageJSON = serde_json::from_str(&del_msg)?;

    let email = Message::builder()
        .from("Site <d4nikla@yandex.ru>".parse()?)
        .to(format!("Client <{}>", msg_struct.email).parse()?)
        .subject(msg_struct.subject)
        .header(ContentType::TEXT_PLAIN)
        .body(msg_struct.message)?;

    if let Err(why) = smtp.send(email).await {
        tracing::error!("Email was not sent: {why}");
    }

    Ok(())
}

pub async fn event_handler() -> anyhow::Result<()> {
    let rmq_con_str = std::env::var("RABBITMQ_URL").unwrap();
    let conn = Connection::connect(&rmq_con_str, ConnectionProperties::default()).await?;

    let max_hardware_concurrency = 1; // TODO CHANGE
    let creds = Credentials::new(
        "d4nikla@yandex.ru".to_owned(),
        std::env::var("YANDEX_PASSWORD").unwrap(),
    );
    let mailer = AsyncSmtpTransport::<Tokio1Executor>::relay("smtp.yandex.ru")?
        .port(465)
        .credentials(creds)
        .timeout(Some(Duration::from_secs(5)))
        .build();

    let mut threads = JoinSet::new();
    for _ in 0..max_hardware_concurrency {
        let channel = conn.create_channel().await?;
        channel.basic_qos(1, BasicQosOptions::default()).await?;
        let mailer = mailer.clone();

        threads.spawn(async move {
            let mut consumer = channel
                .basic_consume(
                    "email-queue",
                    "",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();
            let mailer = mailer.clone();
            while let Some(delivery) = consumer.next().await {
                let Ok(delivery) = delivery else {
                    tracing::error!("No delivery");
                    continue;
                };

                if let Err(why) = handle_delivery(&mailer, &delivery).await {
                    tracing::error!("Processing message failed: {}", why);
                    if let Err(why) = delivery.nack(BasicNackOptions::default()).await {
                        tracing::error!("Processing sending ACK: {}", why);
                    }
                    continue;
                }
                if let Err(why) = delivery.ack(BasicAckOptions::default()).await {
                    tracing::error!("Processing sending ACK: {}", why);
                }
            }
        });
    }
    while let Some(_) = threads.join_next().await {}
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
    tracing::info!("Start");
    event_handler().await
}
