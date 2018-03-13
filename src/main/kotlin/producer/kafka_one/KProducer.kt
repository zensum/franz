package franz.producer.kafka_one

import franz.producer.ProduceResultF
import franz.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader

private fun pairToHeader(p: Pair<String, ByteArray>) =
    RecordHeader(p.first, p.second)

class KProducer<K, V> internal constructor(private val producer: KafkaProducer<K?, V>) : Producer<K, V> {
    internal constructor(opts: Map<String, Any>): this(makeProducer(opts))
    private fun doSend(rec: ProducerRecord<K?, V>) =
        producer.sendAsync(rec).thenApply { KProduceResult.create(it) }
    override fun sendAsync(topic: String, key: K?, value: V): ProduceResultF =
        doSend(ProducerRecord(topic, key, value))
    override fun sendRaw(rec: ProducerRecord<K, V>) =
        doSend(rec as ProducerRecord<K?, V>) // This is fine because K :: K? for all K
    override fun close() =
        producer.close()

    override fun sendAsync(topic: String, key: K?, value: V, headers: Iterable<Pair<String, ByteArray>>): ProduceResultF =
        doSend(ProducerRecord(topic, null, key, value, headers.map(::pairToHeader)))

}