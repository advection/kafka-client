use super::*;
use kafka_rust::error;
use kafka_rust::producer::Record;
use kafka_rust::error::KafkaErrorKind;

/// Tests that basic message sending results in a successful call.
#[tokio::test]
async fn test_producer_send() {
    let mut producer = test_producer().await;
    producer
        .send(&Record::from_value(TEST_TOPIC_NAME, b"foo".as_ref()))
        .await
        .unwrap();
}

/// Sending to a non-existent topic should fail.
#[tokio::test]
async fn test_producer_send_non_existent_topic() {
    let mut producer = test_producer().await;

    let error_code = match producer
        .send(&Record::from_value("non-topic", b"foo".as_ref())).await
        .unwrap_err().kind()
    {
        KafkaErrorKind::Kafka(code) => code,
        other => panic!("Should have received Kafka error instead of: {}", other),
    };

    let correct_error_code = error::KafkaErrorCode::UnknownTopicOrPartition;
    assert_eq!(
        correct_error_code, error_code,
        "should have errored on non-existent topic"
    );
}

/// Simple test for send_all
#[tokio::test]
async fn test_producer_send_all() {
    let mut producer = test_producer().await;
    let records = &[
        Record::from_value(TEST_TOPIC_NAME, b"foo".as_ref()),
        Record::from_value(TEST_TOPIC_NAME, b"bar".as_ref()),
    ];
    let confirms = producer.send_all(records).await.unwrap();

    for confirm in confirms {
        assert_eq!(TEST_TOPIC_NAME.to_owned(), confirm.topic);

        for partition_confirm in confirm.partition_confirms {
            assert!(
                partition_confirm.offset.is_ok(),
                format!(
                    "should have sent successfully. Got: {:?}",
                    partition_confirm.offset
                )
            );
        }
    }
}

/// calling send_all for a non-existent topic should fail
#[tokio::test]
async fn test_producer_send_all_non_existent_topic() {
    let mut producer = test_producer().await;
    let records = &[
        Record::from_value("foo-topic", b"foo".as_ref()),
        Record::from_value("bar-topic", b"bar".as_ref()),
    ];

    let error_code = match producer.send_all(records).await.unwrap_err().kind() {
        KafkaErrorKind::Kafka(code) => code,
        other => panic!("Should have received Kafka error instead of: {}", other),
    };

    let correct_error_code = error::KafkaErrorCode::UnknownTopicOrPartition;
    assert_eq!(
        correct_error_code, error_code,
        "should have errored on non-existent topic"
    );
}
