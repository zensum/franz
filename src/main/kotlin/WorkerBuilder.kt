package franz

import franz.engine.ConsumerActorFactory
import franz.engine.WorkerFunction
import franz.engine.kafka_one.KafkaConsumerActorFactory
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

private val stringDeser = "org.apache.kafka.common.serialization.StringDeserializer"
private val byteArrayDeser = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
private val valueDeserKey = "value.deserializer"

private typealias RunningFunction<T, U> = suspend JobDSL<T, U>.() -> JobStatus
private typealias PipedWorkerFunction<T, U> = suspend (JobState<Message<T, U>>) -> JobStatus

private suspend fun <T, U> runningWorker(fn: RunningFunction<T, U>): WorkerFunction<T, U> = {
    fn(JobDSL(it))
}

private suspend fun <T, U> pipedWorker(fn: PipedWorkerFunction<T, U>, interceptors: List<WorkerInterceptor>): WorkerFunction<T, U> = {
    fn(JobState(it, Stack(), Stack(),interceptors.toList()))
}

/**
 * The number of threads to keep in the pool, even if they are idle
 */
private const val THREAD_POOL_MIN_SIZE: Int = 1
/**
 * The maximum number of threads to allow in the pool
 */
private const val THREAD_POOL_MAX_SIZE: Int = 2
/**
 * When the number of threads is greater than the [THREAD_POOL_MAX_SIZE],
 * this is the maximum time in seconds that excess idle threads will wait
 * for new tasks before terminating
 */
private const val DEFAULT_THREAD_POOL_KEEP_ALIVE_TIME_SECONDS: Long = 30

private fun createDefaultScope(): CoroutineScope =
    createThreadScope(THREAD_POOL_MIN_SIZE, THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_KEEP_ALIVE_TIME_SECONDS)

fun createThreadScope(minPoolSize: Int,maxPoolSize: Int, threadPoolKeepAlive: Long): CoroutineScope {
    val dispatcher: CoroutineDispatcher = ThreadPoolExecutor(
        THREAD_POOL_MIN_SIZE,
        THREAD_POOL_MAX_SIZE,
        DEFAULT_THREAD_POOL_KEEP_ALIVE_TIME_SECONDS,
        TimeUnit.SECONDS,
        ArrayBlockingQueue(50)
    ).asCoroutineDispatcher()
    return CoroutineScope(dispatcher + CoroutineName("franz-coroutine-scope"))
}

data class WorkerBuilder<T> private constructor(
    private val fn: WorkerFunction<String, T>? = null,
    private val opts: Map<String, Any> = emptyMap(),
    private val topics: List<String> = emptyList(),
    private val engine: ConsumerActorFactory = KafkaConsumerActorFactory,
    private val interceptors: List<WorkerInterceptor> = emptyList(),
    private val coroutineScope: CoroutineScope = createDefaultScope()
){
    suspend fun handler(f: WorkerFunction<String, T>) = copy(fn = f)
    @Deprecated("Use piped or handler instead")
    suspend fun running(fn: RunningFunction<String, T>) = handler(runningWorker(fn))
    suspend fun handlePiped(fn: PipedWorkerFunction<String, T>) = handler(pipedWorker(fn, interceptors))
    fun subscribedTo(vararg newTopics: String) = copy(topics = topics + newTopics)
    fun subscribedTo(topics: Collection<String>): WorkerBuilder<T> = merge(this, topics.toTypedArray())
    fun groupId(id: String) = option("group.id", id)
    fun option(k: String, v: Any) = options(mapOf(k to v))
    fun options(newOpts: Map<String, Any>) = copy(opts = opts + newOpts)
    fun coroutineScope(coroutineScope: CoroutineScope): WorkerBuilder<T> = copy(coroutineScope = coroutineScope)
    fun setEngine(e: ConsumerActorFactory): WorkerBuilder<T> = copy(engine = e)

    fun install(i: WorkerInterceptor): WorkerBuilder<T> = copy(interceptors = interceptors.toMutableList().also{ it.add(i)})

    fun getInterceptors() = interceptors

    fun start() {
        val c = engine.create<String, T>(opts, topics)
        c.createWorker(fn!!, coroutineScope)
        c.start()
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