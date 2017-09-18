package franz.producer.kafka_one

import franz.producer.ProduceResultF
import franz.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

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