package frans.producer.mock

import franz.producer.ProduceResult
import franz.producer.ProduceResultF
import franz.producer.Producer

import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.CompletableFuture

class MockProducer<K, V>(
    val doSendResult: ProduceResult = MockProduceResult(),
    val sendAsyncResult: ProduceResult = MockProduceResult(),
    val sendRawResult: ProduceResult = MockProduceResult(),
    val onSend: (V) -> Unit = {},
    val onSendAsync: (V) -> Unit = {},
    val onSendRaw: (V) -> Unit = {}

): Producer<K, V> {
    private fun doSend(rec: ProducerRecord<K, V>) =
        CompletableFuture.supplyAsync { doSendResult }.also { onSend.invoke(rec.value()) }
    override fun sendAsync(topic: String, key: K?, value: V): ProduceResultF =
        CompletableFuture.supplyAsync { sendAsyncResult }.also { onSendAsync.invoke(value) }
    override fun sendRaw(rec: ProducerRecord<K, V>) =
        CompletableFuture.supplyAsync { sendRawResult }.also { onSendRaw.invoke(rec.value()) }
    override fun close() = Unit

    fun createFactory() =
        MockProducerFactory(this)
}