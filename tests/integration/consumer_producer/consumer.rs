use super::*;

use kafka_rust::error;
use kafka_rust::producer::Record;

use env_logger;
use kafka_rust::error::KafkaErrorKind;

/// Tests that consuming one message works
#[tokio::test]
async fn test_consumer_poll() {
    // poll once to set a position in the topic
    let mut consumer = test_consumer().await;
    let mut messages = consumer.poll().await.unwrap();
    assert!(
        messages.is_empty(),
        "messages was not empty: {:?}",
        messages
    );

    // send a message and then poll it and ensure it is the correct message
    let correct_message_contents = b"test_consumer_poll";
    let mut producer = test_producer().await;
    producer
        .send(&Record::from_value(
            TEST_TOPIC_NAME,
            correct_message_contents.as_ref(),
        ))
        .await
        .unwrap();

    messages = consumer.poll().await.unwrap();
    let mut messages_iter = messages.iter();
    let message_set = messages_iter.next().unwrap();

    assert_eq!(
        1,
        message_set.messages().len(),
        "should only be one message"
    );

    let message_content = message_set.messages()[0].value;
    assert_eq!(
        correct_message_contents, message_content,
        "incorrect message contents"
    );
}

/// Test Consumer::commit_messageset
#[tokio::test]
async fn test_consumer_commit_messageset() {
    let _ = env_logger::try_init();

    let mut consumer = test_consumer().await;

    // get the offsets at the beginning of the test
    let start_offsets = get_group_offsets(
        &mut new_ready_kafka_client().await,
        TEST_GROUP_NAME,
        TEST_TOPIC_NAME,
        Some(0),
    ).await;

    debug!("start offsets: {:?}", start_offsets);

    // poll once to set a position in the topic
    let messages = consumer.poll().await.unwrap();
    assert!(
        messages.is_empty(),
        "messages was not empty: {:?}",
        messages
    );

    // send some messages to the topic
    const NUM_MESSAGES: i64 = 100;
    send_random_messages(&mut test_producer().await, TEST_TOPIC_NAME, NUM_MESSAGES as u32).await;

    let mut num_messages = 0;

    'read: loop {
        for ms in consumer.poll().await.unwrap().iter() {
            let messages = ms.messages();
            num_messages += messages.len();
            consumer.consume_messageset(ms).unwrap();
        }

        consumer.commit_consumed().await.unwrap();

        if num_messages >= (NUM_MESSAGES as usize) {
            break 'read;
        }
    }

    assert_eq!(
        NUM_MESSAGES as usize, num_messages,
        "wrong number of messages"
    );

    // get the latest offsets and make sure they add up to the number of messages
    let latest_offsets = get_group_offsets(
        &mut new_ready_kafka_client().await,
        TEST_GROUP_NAME,
        TEST_TOPIC_NAME,
        Some(0),
    ).await;

    debug!("end offsets: {:?}", latest_offsets);

    // add up the differences
    let num_new_messages_committed = diff_group_offsets(&start_offsets, &latest_offsets);

    assert_eq!(
        NUM_MESSAGES, num_new_messages_committed,
        "wrong number of messages committed"
    );

    for partition in consumer.subscriptions().get(TEST_TOPIC_NAME).unwrap() {
        let consumed_offset = consumer
            .last_consumed_message(TEST_TOPIC_NAME, *partition)
            .unwrap();
        let latest_offset = latest_offsets.get(&partition).unwrap();
        assert_eq!(
            *latest_offset - 1,
            consumed_offset,
            "latest consumed offset is incorrect"
        );
    }
}

/// Verify that if Consumer::commit_consumed is called without consuming any
/// message sets, nothing is committed.
#[tokio::test]
async fn test_consumer_commit_messageset_no_consumes() {
    let _ = env_logger::try_init();

    let mut consumer = test_consumer().await;

    // get the offsets at the beginning of the test
    let start_offsets = get_group_offsets(
        &mut new_ready_kafka_client().await,
        TEST_GROUP_NAME,
        TEST_TOPIC_NAME,
        Some(0),
    ).await;

    debug!("start offsets: {:?}", start_offsets);

    // poll once to set a position in the topic
    let messages = consumer.poll().await.unwrap();
    assert!(
        messages.is_empty(),
        "messages was not empty: {:?}",
        messages
    );

    // send some messages to the topic
    const NUM_MESSAGES: i64 = 100;
    send_rand   om_messages(&mut test_producer().await, TEST_TOPIC_NAME, NUM_MESSAGES as u32).await;

    let mut num_messages = 0;

    'read: loop {
        for ms in consumer.poll().await.unwrap().iter() {
            let messages = ms.messages();
            num_messages += messages.len();

            // DO NOT consume the messages
            // consumer.consume_messageset(ms).unwrap();
        }

        consumer.commit_consumed().await.unwrap();

        if num_messages >= (NUM_MESSAGES as usize) {
            break 'read;
        }
    }

    assert_eq!(
        NUM_MESSAGES as usize, num_messages,
        "wrong number of messages"
    );

    // get the latest offsets and make sure they add up to the number of messages
    let latest_offsets = get_group_offsets(
        &mut consumer.into_client(),
        TEST_GROUP_NAME,
        TEST_TOPIC_NAME,
        Some(0),
    ).await;

    debug!("end offsets: {:?}", latest_offsets);

    // add up the differences
    let num_new_messages_committed = diff_group_offsets(&start_offsets, &latest_offsets);

    // without consuming any messages, the diff should be 0
    assert_eq!(
        0, num_new_messages_committed,
        "wrong number of messages committed"
    );
}

/// Consuming from a non-existent topic should fail.
#[tokio::test]
async fn test_consumer_non_existent_topic() {
    let consumer_err = test_consumer_builder()
        .with_topic_partitions("foo_topic".to_owned(), &TEST_TOPIC_PARTITIONS)
        .create()
        .await
        .unwrap_err();

    let error_code = match consumer_err.kind() {
        KafkaErrorKind::Kafka(code) => code,
        other => panic!("Should have received Kafka error instead of: {}", other),
    };

    let correct_error_code = error::KafkaErrorCode::UnknownTopicOrPartition;
    assert_eq!(
        correct_error_code, error_code,
        "should have errored on non-existent topic"
    );
}
