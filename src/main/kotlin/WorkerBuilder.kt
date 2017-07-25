package franz

import franz.internal.WorkerFunction
import franz.internal.kafkaConsumer
import franz.internal.ConsumerActor
import franz.internal.createWorkers

private val stringDeser = "org.apache.kafka.common.serialization.StringDeserializer"
private val byteArrayDeser = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
private val valueDeserKey = "value.deserializer"

data class WorkerBuilder<T> private constructor(private val fn: WorkerFunction<String, T>? = null,
                         private val opts: Map<String, Any> = emptyMap(),
                         private val topics: List<String> = emptyList(),
                         private val nThreads: Int = 1) {

    fun running(fn: WorkerFunction<String, T>) = copy(fn = fn)
    fun subscribedTo(vararg newTopics: String) = copy(topics = topics + newTopics)
    fun groupId(id: String) = option("group.id", id)
    fun parallelism(n: Int) = copy(nThreads = n)
    fun option(k: String, v: Any) = options(mapOf(k to v))
    fun options(newOpts: Map<String, Any>) = copy(opts = opts + newOpts)

    fun start() {
        val c = ConsumerActor<String, T>(kafkaConsumer(opts, topics))
        createWorkers(nThreads, c, fn!!)
        c.start()
    }
    companion object {
        val ofByteArray = WorkerBuilder<ByteArray>().option(valueDeserKey, stringDeser)
        val ofString = WorkerBuilder<String>().option(valueDeserKey, byteArrayDeser)
    }
}

