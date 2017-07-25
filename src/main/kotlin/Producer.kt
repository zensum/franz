package franz
import franz.internal.createKafkaConfig
import kotlinx.coroutines.experimental.future.await
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.CompletableFuture

private val sensibleDefaults = mapOf(
        "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
        "acks" to "all",
        "compression.type" to "gzip",
        "request.timeout.ms" to "10000",
        "max.block.ms" to "5000",
        "retries" to "0"
)

private fun makeConfig(userOpts: Map<String, Any>) = createKafkaConfig(userOpts, sensibleDefaults)
private fun <K, V> makeProducer(userOpts: Map<String, Any>) = KafkaProducer<K, V>(makeConfig(userOpts))


private val stringSer = "org.apache.kafka.common.serialization.StringSerializer"
private val byteArraySer = "org.apache.kafka.common.serialization.ByteArraySerializer"
private val valueSerKey = "value.serializer"

class ProducerBuilder<T> private constructor(private val opts: Map<String, Any> = emptyMap()) {
    fun option(k: String, v: Any) = options(mapOf(k to v))
    fun options(newOpts: Map<String, Any>) = ProducerBuilder<T>(opts + newOpts)
    fun create(): Producer<String, T> = Producer(makeProducer<String, T>(opts))
    companion object {
        val ofByteArray = ProducerBuilder<ByteArray>().option(valueSerKey, stringSer)
        val ofString = ProducerBuilder<String>().option(valueSerKey, byteArraySer)
    }
}


class Producer<K, V> internal constructor(private val producer: KafkaProducer<K, V>) {
    suspend fun sendRaw(rec: ProducerRecord<K, V>) = producer.sendAsync(rec)
    suspend fun send(topic: String, key: K, value: V) = sendRaw(ProducerRecord(topic, key, value))
    fun close() = producer.close()
    fun forTopic(topic: String) = TopicProducer(this, topic)
}

class TopicProducer<in K, in V> internal constructor(private val producer: Producer<K, V>, private val topic: String) {
    suspend fun send(key: K, value: V) = producer.send(topic, key, value).await()
    suspend fun send(value: V) = producer.sendRaw(ProducerRecord(topic, value)).await()
}

private fun <K, V> KafkaProducer<K, V>.sendAsync(record: ProducerRecord<K, V>) = FutureCallback().also {
    this.send(record, it)
}.cf

suspend fun <K, V> KafkaProducer<K, V>.sendAwait(record: ProducerRecord<K, V>) = sendAsync(record).await()

private class FutureCallback : Callback {
    val cf = CompletableFuture<RecordMetadata>()
    override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
        when (exception) {
            null -> cf.complete(metadata)
            else -> cf.completeExceptionally(exception)
        }
    }
}