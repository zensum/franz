package franz.internal

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

fun <T, U> ConsumerRecord<T, U>.topicPartition() =
        TopicPartition(topic(), partition())

fun <T, U> ConsumerRecord<T, U>.jobId() = topicPartition() to offset()