package franz

import franz.engine.ConsumerActorFactory
import franz.engine.WorkerFunction
import franz.engine.kafka_one.KafkaConsumerActorFactory

private val stringDeser = "org.apache.kafka.common.serialization.StringDeserializer"
private val byteArrayDeser = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
private val valueDeserKey = "value.deserializer"

private typealias RunningFunction<T, U> = suspend JobDSL<T, U>.() -> JobStatus
private typealias PipedWorkerFunction<T, U> = suspend (JobState<Message<T, U>>) -> JobStatus

private fun <T, U> runningWorker(fn: RunningFunction<T, U>): WorkerFunction<T, U> = {
    fn(JobDSL(it))
}

private fun <T, U> pipedWorker(fn: PipedWorkerFunction<T, U>, interceptors: List<WorkerInterceptor>): WorkerFunction<T, U> = {
    fn(JobState(it, interceptors))
}

data class WorkerBuilder<T> private constructor(
    private val fn: WorkerFunction<String, T>? = null,
    private val opts: Map<String, Any> = emptyMap(),
    private val topics: List<String> = emptyList(),
    private val engine: ConsumerActorFactory = KafkaConsumerActorFactory,
    private val interceptors: List<WorkerInterceptor> = emptyList()
){
    fun handler(f: WorkerFunction<String, T>) = copy(fn = f)
    @Deprecated("Use piped or handler instead")
    fun running(fn: RunningFunction<String, T>) = handler(runningWorker(fn))
    fun handlePiped(fn: PipedWorkerFunction<String, T>) = handler(pipedWorker(fn, interceptors))
    fun subscribedTo(vararg newTopics: String) = copy(topics = topics + newTopics)
    fun subscribedTo(topics: Collection<String>): WorkerBuilder<T> = merge(this, topics.toTypedArray())
    fun groupId(id: String) = option("group.id", id)
    fun option(k: String, v: Any) = options(mapOf(k to v))
    fun options(newOpts: Map<String, Any>) = copy(opts = opts + newOpts)
    fun setEngine(e: ConsumerActorFactory): WorkerBuilder<T> = copy(engine = e)

    fun install(i: WorkerInterceptor): WorkerBuilder<T> = copy(interceptors = interceptors.toMutableList().also{ it.add(i)})

    fun getInterceptors() = interceptors

    fun start(){
        val c = engine.create<String, T>(opts, topics)
        setupInterceptors()
        c.createWorker(fn!!)
        c.start()
    }

    private fun setupInterceptors(){
        if(interceptors.size > 2){
            for(i in 1 .. interceptors.size){
                interceptors[i -1].next = interceptors[i]
            }
        }
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

