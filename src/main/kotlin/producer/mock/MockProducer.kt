package frans.producer.mock

import franz.producer.ProduceResult
import franz.producer.ProduceResultF
import franz.producer.Producer
import franz.producer.kafka_one.KProduceResult
import franz.producer.kafka_one.makeProducer
import franz.producer.kafka_one.sendAsync
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.CompletableFuture

class MockProducer<K, V>(
    val doSendResult: ProduceResult = MockProduceResult(),
    val sendAsyncResult: ProduceResult = MockProduceResult(),
    val sendRawResult: ProduceResult = MockProduceResult()
): Producer<K, V> {
    private fun doSend(rec: ProducerRecord<K, V>) =
        CompletableFuture.supplyAsync { doSendResult }
    override fun sendAsync(topic: String, key: K?, value: V): ProduceResultF =
        CompletableFuture.supplyAsync { sendAsyncResult }
    override fun sendRaw(rec: ProducerRecord<K, V>) =
        CompletableFuture.supplyAsync { sendRawResult }
    override fun close() = Unit

}