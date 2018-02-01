package franz

import franz.engine.ConsumerActor
import franz.engine.ConsumerActorFactory
import franz.engine.kafka_one.KafkaConsumerActorFactory

private val stringDeser = "org.apache.kafka.common.serialization.StringDeserializer"
private val byteArrayDeser = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
private val valueDeserKey = "value.deserializer"

private typealias RunningFunction<T, U> = suspend JobDSL<T, U>.() -> JobStatus
private typealias PipedWorkerFunction<T, U> = suspend (JobState<Message<T, U>>) -> JobStatus

private fun <T, U> runningWorker(fn: RunningFunction<T, U>): WorkerFunction<T, U> = {
    fn(JobDSL(it))
}

private fun <T, U> pipedWorker(fn: PipedWorkerFunction<T, U>): WorkerFunction<T, U> = {
    fn(JobState(it))
}

data class Worker<K, V>(
    private val consumer: ConsumerActor<K, V>,
    private val thread: Thread
){
    fun stop(){
        consumer.stop()
        thread.interrupt()
    }
}

data class WorkerBuilder<T> private constructor(
    private val fn: WorkerFunction<String, T>? = null,
    private val opts: Map<String, Any> = emptyMap(),
    private val topics: List<String> = emptyList(),
    private val engine: ConsumerActorFactory = KafkaConsumerActorFactory) {

    fun handler(f: WorkerFunction<String, T>) = copy(fn = f)
    @Deprecated("Use piped or handler instead")
    fun running(fn: RunningFunction<String, T>) = handler(runningWorker(fn))
    fun handlePiped(fn: PipedWorkerFunction<String, T>) = handler(pipedWorker(fn))
    fun subscribedTo(vararg newTopics: String) = copy(topics = topics + newTopics)
    fun subscribedTo(topics: Collection<String>): WorkerBuilder<T> = merge(this, topics.toTypedArray())
    fun groupId(id: String) = option("group.id", id)
    fun option(k: String, v: Any) = options(mapOf(k to v))
    fun options(newOpts: Map<String, Any>) = copy(opts = opts + newOpts)
    fun setEngine(e: ConsumerActorFactory): WorkerBuilder<T> = copy(engine = e)

    fun start(): Worker<String, T> {
        val c = engine.create<String, T>(opts, topics)
        val th = createWorker(c, fn!!)
        th.start()
        c.start()

        return Worker(c, th)
    }

    private tailrec fun merge(builder: WorkerBuilder<T>, topics: Array<String>, i: Int = 0): WorkerBuilder<T> {
        return when(i > topics.lastIndex) {
            true -> builder
            false -> merge(builder.subscribedTo(topics[i]), topics, i+1)
        }
    }

    companion object {
        val ofByteArray = WorkerBuilder<ByteArray>().option(valueDeserKey, byteArrayDeser)
        val ofString = WorkerBuilder<String>().option(valueDeserKey, stringDeser)
    }
}

