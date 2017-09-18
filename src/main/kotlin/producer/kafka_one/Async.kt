package franz.producer.kafka_one

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.CompletableFuture


internal fun <K, V> KafkaProducer<K, V>.sendAsync(record: ProducerRecord<K, V>) = FutureCallback().also {
    this.send(record, it)
}.cf

private class FutureCallback : Callback {
    val cf = CompletableFuture<RecordMetadata>()
    override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
        when (exception) {
            null -> cf.complete(metadata)
            else -> cf.completeExceptionally(exception)
        }
    }
}