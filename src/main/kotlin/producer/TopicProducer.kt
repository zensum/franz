package franz.producer

class TopicProducer<in K, in V> internal constructor(
    private val producer: Producer<K, V>,
    private val topic: String) {

    fun sendAsync(key: K, value: V) = producer.sendAsync(topic, key, value)
    fun sendAsync(value: V) = producer.sendAsync(topic, value)

    suspend fun send(key: K?, value: V) = producer.send(topic, key, value)
    suspend fun send(value: V) = producer.send(topic, value)
}

