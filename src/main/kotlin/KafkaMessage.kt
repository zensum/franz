package franz.internal

import franz.Message
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaMessage<T, U>(private val rec: ConsumerRecord<T, U>) : Message<T, U> {
    override fun value(): U = rec.value()
    override fun key(): T = rec.key()
    override fun headers(): Array<Pair<String, ByteArray>> = TODO("not implemented")
    override fun headers(key: String): Array<ByteArray> = TODO("not implemented")
    override fun jobId(): JobId = rec.jobId()
}