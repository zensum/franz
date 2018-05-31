package frans.producer.mock

import franz.producer.ProduceResult
import franz.producer.ProduceResultF
import franz.producer.Producer

import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.supplyAsync

class MockProducer<K, V>(
    val doSendResult: ProduceResult = MockProduceResult(),
    val sendAsyncResult: ProduceResult = MockProduceResult(),
    val sendRawResult: ProduceResult = MockProduceResult(),
    val onSend: (V) -> Unit = {},
    val onSendAsync: (V) -> Unit = {},
    val onSendRaw: (V) -> Unit = {}
): Producer<K, V> {
    private val results: MutableList<V> = mutableListOf()
    private fun doSend(rec: ProducerRecord<K, V>): CompletableFuture<ProduceResult> {
        results.add(rec.value())
        onSend.invoke(rec.value())
        return supplyAsync { doSendResult }
    }

    override fun sendAsync(topic: String, key: K?, value: V): ProduceResultF {
        results.add(value)
        onSendAsync.invoke(value)
        return supplyAsync { sendAsyncResult }
    }

    override fun sendRaw(rec: ProducerRecord<K, V>): CompletableFuture<ProduceResult> {
        results.add(rec.value())
        onSendRaw.invoke(rec.value())
        return supplyAsync { sendRawResult }
    }

    override fun sendAsync(topic: String, key: K?, value: V, headers: Iterable<Pair<String, ByteArray>>): ProduceResultF {
        results.add(value)
        onSendAsync.invoke(value)
        return supplyAsync { sendAsyncResult }
    }
    override fun close() = Unit

    fun createFactory() =
        MockProducerFactory(this)

    fun getResults(): List<V> =
        results.toList()
}