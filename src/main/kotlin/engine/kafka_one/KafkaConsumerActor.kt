package franz.engine.kafka_one

import franz.JobStateException
import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor
import franz.engine.WorkerFunction
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.Comparator
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

typealias JobId = Pair<TopicPartition, Long>
typealias SetJobStatus = Pair<JobId, JobStatus>

private fun <T> drainQueue(bq: BlockingQueue<T>): List<T> =
    mutableListOf<T>()
        .also { bq.drainTo(it) }
        .toList()

const val POLLING_INTERVAL = 30000L

private fun <T, U> commitFinishedJobs(
    c: KafkaConsumer<T, U>,
    statuses: JobStatuses<T, U>
): JobStatuses<T, U> =
    statuses.committableOffsets().also {
        logger.info { "Pushing new offsets for ${it.count()} partitions" }
        c.commitAsync(it) { res, exc ->
            if (exc != null) {
                logger.error(exc) { "Crashed while committing: $res" }
            }
        }
    }.let {
        statuses.removeCommitted(it)
    }

private fun <T, U> fetchMessagesFromKafka(
    c: KafkaConsumer<T, U>,
    outQueue: BlockingQueue<ConsumerRecord<T, U>>,
    jobStatuses: JobStatuses<T, U>
): Pair<List<ConsumerRecord<T, U>>, JobStatuses<T, U>> =
    c.poll(POLLING_INTERVAL).let {
        if (it.count() > 0) {
            logger.info { "Adding ${it.count()} new tasks from Kafka" }
        } else {
            logger.debug { "Adding no new tasks from Kafka" }
        }
        logger.debug { "Size outQueue: ${outQueue.size}, remaining capacity: ${outQueue.remainingCapacity()}" }
        outQueue.offerAll(it) to jobStatuses.addJobs(it)
    }

private fun <T> BlockingQueue<T>.offerAll(xs: Iterable<T>): List<T> = xs.dropWhile { offer(it) }

private fun <T, U> retryTransientFailures(
    outQueue: BlockingQueue<ConsumerRecord<T, U>>,
    jobStatuses: JobStatuses<T, U>
): Pair<List<ConsumerRecord<T, U>>, JobStatuses<T, U>> =
    jobStatuses.rescheduleTransientFailures().let { (newJobStatuses, producerRecs) ->
        if (producerRecs.isNotEmpty()) {
            logger.info { "Retrying ${producerRecs.count()} tasks" }
        }
        outQueue.offerAll(producerRecs) to newJobStatuses
    }

private fun <T, U> processCommandQueue(
    c: KafkaConsumer<T, U>,
    jobStatuses: JobStatuses<T, U>,
    commandQueue: BlockingQueue<SetJobStatus>
): JobStatuses<T, U> = drainQueue(commandQueue).let {
    if (it.isNotEmpty()) {
        commitFinishedJobs(c, jobStatuses.update(it.toMap()))
    } else {
        jobStatuses
    }
}

private tailrec fun <T, U> writeRemainder(
    rem: List<ConsumerRecord<T, U>>,
    c: KafkaConsumer<T, U>,
    jobStatuses: JobStatuses<T, U>,
    commandQueue: BlockingQueue<SetJobStatus>,
    outQueue: BlockingQueue<ConsumerRecord<T, U>>
): JobStatuses<T, U> =
    if (rem.size == 0) {
        jobStatuses
    } else {
        val newJobStatuses = iterate({ !outQueue.offer(rem.first()) }, jobStatuses) {
            if (commandQueue.size == 0) {
                Thread.sleep(5)
            }
            processCommandQueue(c, it, commandQueue)
        }
        writeRemainder(rem.drop(1), c, newJobStatuses, commandQueue, outQueue)
    }

private fun <T, U> writeRemainder(prev: Pair<List<ConsumerRecord<T, U>>, JobStatuses<T, U>>,
                                  c: KafkaConsumer<T, U>,
                                  commandQueue: BlockingQueue<SetJobStatus>,
                                  outQueue: BlockingQueue<ConsumerRecord<T, U>>) =
    writeRemainder(prev.first, c, prev.second, commandQueue, outQueue)

private fun sequenceWhile(fn: () -> Boolean): Sequence<Unit> =
    object : Iterator<Unit> {
        override fun next() = Unit
        override fun hasNext(): Boolean = fn()
    }.asSequence()

private fun <T> iterate(whileFn: () -> Boolean, initialValue: T, fn: (T) -> T) =
    sequenceWhile(whileFn).fold(initialValue) { acc, _ -> fn(acc) }

private fun <T, U> consumerLoop(
    c: KafkaConsumer<T, U>,
    outQueue: BlockingQueue<ConsumerRecord<T, U>>,
    commandQueue: BlockingQueue<SetJobStatus>,
    run: () -> Boolean
): JobStatuses<T, U> =
    iterate(run, JobStatuses<T, U>()) {
        processCommandQueue(c, it, commandQueue)
            .let {
                fetchMessagesFromKafka(c, outQueue, it).let {
                    writeRemainder(it, c, commandQueue, outQueue)
                }
            }
            .let { statuses: JobStatuses<T, U> ->
                retryTransientFailures(outQueue, statuses).let {
                        p:  Pair<List<ConsumerRecord<T, U>>, JobStatuses<T, U>> ->
                    writeRemainder(p, c, commandQueue, outQueue)
                }
            }
    }

const val COMMAND_QUEUE_DEPTH = 1000
const val MESSAGE_QUEUE_DEPTH = 1000

class KafkaConsumerActor<T, U>(private val kafkaConsumer: KafkaConsumer<T, U>) : ConsumerActor<T, U> {
    constructor(opts: Map<String, Any>, topics: List<String>): this(kafkaConsumer<T, U>(opts, topics))
    private val outQueue = PriorityBlockingQueue<ConsumerRecord<T, U>>(MESSAGE_QUEUE_DEPTH, ConsumerRecordComparator)
    private val commandQueue = PriorityBlockingQueue<SetJobStatus>(COMMAND_QUEUE_DEPTH, SetJobStatusComparator)
    private val runFlag = AtomicBoolean(true)
    private fun createThread() =
        Thread({ consumerLoop(kafkaConsumer, outQueue, commandQueue, runFlag::get) }).also { logger.info { "Creating thread..." } }
    override fun start() {
        createThread().start()
    }
    fun take() = KafkaMessage(outQueue.take())
    override fun subscribe(fn: (Message<T, U>) -> Unit) {
        while (true) {
            fn(take())
        }
    }
    override fun stop() = runFlag.lazySet(false)
    override fun setJobStatus(message: Message<T, U>, status: JobStatus) {
        commandQueue.put(SetJobStatus((message as KafkaMessage).jobId(), status))
        logger.debug { "Set JobStatus in command queue " }
    }

    override fun createWorker(
        fn: WorkerFunction<T, U>,
        scope: CoroutineScope
    ){
        Thread { worker(this, fn, scope)}.start()
    }

    private inline fun tryJobStatus(fn: () -> JobStatus) = try {
        fn()
    } catch(ex: JobStateException){
        logger.error("Job threw an exception", ex)
        ex.result
    } catch (ex: Exception) {
        logger.error("Job threw an exception", ex)
        JobStatus.TransientFailure
    }

    private fun <T, U> worker(
        consumer: ConsumerActor<T, U>,
        fn: WorkerFunction<T, U>,
        scope: CoroutineScope
    ){
        try {
            consumer.subscribe {
                logger.trace { "Consumer is subscribing" }
                scope.launch(scope.coroutineContext) {
                    logger.trace { "Launching consumer" }
                    consumer.setJobStatus(it, tryJobStatus {
                        logger.trace { "Executing worker function" }
                        fn(it).also {
                            logger.trace { "Worker function executed with result: $it" }
                        }
                    })
                }
            }
        }catch (ex: Exception){
            logger.error ("Starting worker threw an exception", ex)
        }
    }
}

object ConsumerRecordComparator: Comparator<ConsumerRecord<*, *>> {
    override fun compare(p0: ConsumerRecord<*, *>, p1: ConsumerRecord<*, *>): Int {
        val sameTopic: Boolean = p0.topic() == p1.topic()
        val samePartition: Boolean = p0.partition() == p1.partition()
        val diffTimestamp: Int = p0.timestamp().compareTo(p1.timestamp())
        val diffOffset: Int = p0.partition().compareTo(p1.offset())
        return when {
            !sameTopic -> diffTimestamp
            !samePartition -> diffTimestamp
            else -> diffOffset
        }
    }
}

private object SetJobStatusComparator: Comparator<SetJobStatus> {
    override fun compare(p0: SetJobStatus, p1: SetJobStatus): Int {
        val sameTopic: Boolean = p0.first.first.topic() == p1.first.first.topic()
        val samePartition: Boolean = p0.first.first.partition() == p1.first.first.partition()
        val p0Offset: Long = p0.first.second
        val p1Offset: Long = p1.first.second
        return when {
            p0.second.mayRetry() && !p1.second.mayRetry() -> -1
            !p0.second.mayRetry() && p1.second.mayRetry() -> 1
            !sameTopic -> 0
            !samePartition -> 0
            else -> p0Offset.compareTo(p1Offset)
        }
    }
}