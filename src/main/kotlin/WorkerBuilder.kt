package franz

import franz.internal.WorkerFunction
import franz.internal.kafkaConsumer
import franz.internal.ConsumerActor
import franz.internal.createWorkers

data class WorkerBuilder(private val fn: WorkerFunction<String, String>? = null,
                         private val opts: Map<String, Any> = emptyMap(),
                         private val topics: List<String> = emptyList(),
                         private val nThreads: Int = 1) {

    fun running(fn: WorkerFunction<String, String>) = copy(fn = fn)
    fun subscribedTo(topic: String) = copy(topics = topics + topic)
    fun parallelism(n: Int) = copy(nThreads = n)
    fun option(k: String, v: Any) = options(mapOf(k to v))
    fun options(newOpts: Map<String, Any>) = copy(opts = opts + newOpts)

    fun start() {
        val c = ConsumerActor(kafkaConsumer(opts, topics))
        createWorkers(nThreads, c, fn!!)
        c.start()
    }
}