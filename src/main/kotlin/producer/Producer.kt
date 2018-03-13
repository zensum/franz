package franz.producer

import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.experimental.future.await

typealias ProduceResultF = CompletableFuture<ProduceResult>



interface Producer<K, V> {
    @Deprecated("This API relies on directly on Kafka and will be removed")
    fun sendRaw(rec: ProducerRecord<K, V>): ProduceResultF
    fun sendAsync(topic: String, key: K?, value: V): ProduceResultF

    fun sendAsync(topic: String, key: K?, value: V, headers: Iterable<Pair<String, ByteArray>>) : ProduceResultF
    fun sendAsync(topic: String, key: K?, value: V, headers: Map<String, ByteArray>) =
        sendAsync(topic, key, value, headers.toList())
    fun close(): Unit

    fun sendAsync(topic: String, value: V): ProduceResultF =
        sendAsync(topic, null, value)

    fun forTopic(topic: String) = TopicProducer(this, topic)
    suspend fun send(topic: String, key: K?, value: V): ProduceResult =
        sendAsync(topic, key, value).await()
    suspend fun send(topic: String, value: V): ProduceResult =
        sendAsync(topic, value).await()


    suspend fun send(topic: String, key: K?, value: V, headers: Iterable<Pair<String, ByteArray>>) =
        sendAsync(topic, key, value, headers).await()

    suspend fun send(topic: String, key: K?, value: V, headers: Map<String, ByteArray>) =
        sendAsync(topic, key, value, headers).await()
}