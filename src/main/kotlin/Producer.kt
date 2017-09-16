package franz

// This producer has a hard dependency that needs fixing.
import franz.engine.kafka_one.createKafkaConfig
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
    fun create(): Producer<String, T> = KProducer(opts)
    companion object {
        val ofByteArray = ProducerBuilder<ByteArray>().option(valueSerKey, byteArraySer)
        val ofString = ProducerBuilder<String>().option(valueSerKey, stringSer)
    }
}

interface ProduceResult {
    fun offset(): Long
    fun timestamp(): Long
    fun topic(): String
    fun partition(): Int
}

typealias ProduceResultF = CompletableFuture<ProduceResult>

interface Producer<K, V> {
    @Deprecated("This API relies on directly on Kafka and will be removed")
    fun sendRaw(rec: ProducerRecord<K, V>): ProduceResultF
    fun sendAsync(topic: String, key: K?, value: V): ProduceResultF
    fun close() : Unit

    fun sendAsync(topic: String, value: V): ProduceResultF =
        sendAsync(topic, null, value)

    fun forTopic(topic: String) = TopicProducer(this, topic)
    suspend fun send(topic: String, key: K?, value: V): ProduceResult =
        sendAsync(topic, key, value).await()
    suspend fun send(topic: String, value: V): ProduceResult =
        sendAsync(topic, value).await()
}

class TopicProducer<in K, in V> internal constructor(private val producer: Producer<K, V>, private val topic: String) {
    fun sendAsync(key: K, value: V) = producer.sendAsync(topic, key, value)
    fun sendAsync(value: V) = producer.sendAsync(topic, value)

    suspend fun send(key: K?, value: V) = producer.send(topic, key, value)
    suspend fun send(value: V) = producer.send(topic, value)
}


class KProduceResult private constructor(private val md: RecordMetadata) : ProduceResult {
    override fun offset() = md.offset()
    override fun partition() = md.partition()
    override fun timestamp() = md.timestamp()
    override fun topic() = md.topic()!!
    companion object {
        fun create(md: RecordMetadata): ProduceResult = KProduceResult(md)
    }
}

class KProducer<K, V> internal constructor(private val producer: KafkaProducer<K, V>) : Producer<K, V> {
    internal constructor(opts: Map<String, Any>): this(makeProducer(opts))
    private fun doSend(rec: ProducerRecord<K, V>) =
        producer.sendAsync(rec).thenApply { KProduceResult.create(it) }
    override fun sendAsync(topic: String, key: K?, value: V): ProduceResultF =
        doSend(ProducerRecord<K, V>(topic, key, value))
    override fun sendRaw(rec: ProducerRecord<K, V>) =
        doSend(rec)
    override fun close() =
        producer.close()
}

private fun <K, V> KafkaProducer<K, V>.sendAsync(record: ProducerRecord<K, V>) = FutureCallback().also {
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