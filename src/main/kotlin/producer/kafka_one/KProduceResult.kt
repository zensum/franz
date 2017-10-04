package franz.producer.kafka_one

import franz.producer.ProduceResult
import org.apache.kafka.clients.producer.RecordMetadata

class KProduceResult private constructor(private val md: RecordMetadata) : ProduceResult {
    override fun offset() = md.offset()
    override fun partition() = md.partition()
    override fun timestamp() = md.timestamp()
    override fun topic() = md.topic()!!
    companion object {
        fun create(md: RecordMetadata): ProduceResult = KProduceResult(md)
    }
}