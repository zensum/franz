package frans.producer.mock

import franz.producer.ProduceResult

class MockProduceResult(
    val offset: Long = 0,
    val partition: Int = 0,
    val timestamp: Long = 0,
    val topic: String = ""
) : ProduceResult {
    override fun offset() = offset
    override fun partition() = partition
    override fun timestamp() = timestamp
    override fun topic() = topic
}
