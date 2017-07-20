package franz.internal
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

data class SetJobStatus(val id: Pair<TopicPartition, Long>, val status: JobStatus)

sealed class JobStatus {
    object Success : JobStatus()
    data class TransientFailure(val throwable: Throwable) : JobStatus()
    data class PermanentFailure(val throwable: Throwable) : JobStatus()
    object Incomplete : JobStatus()
    object Retry : JobStatus()
    fun isDone() = when(this) {
        is Success -> true
        is TransientFailure -> false
        is PermanentFailure -> true
        is Incomplete -> false
        is Retry -> false
    }
    fun mayRetry() = when(this) {
        is TransientFailure -> true
        else -> false
    }
}

typealias JobId = Pair<TopicPartition, Long>

private fun processSetJobStatusMessages(cmds: List<SetJobStatus>) : Map<JobId, JobStatus> = cmds
        .map { (id, status) -> mapOf(id to status) }
        .plusElement(emptyMap()) // reduce needs non-zero cardinality
        .reduce { a, b -> a + b }



private fun <T> drainQueue(bq: BlockingQueue<T>): List<T> =
        mutableListOf<T>()
                .also { bq.drainTo(it) }
                .toList()


const val POLLING_INTERVAL = 10000L

private fun <T, U> commitFinishedJobs(c: KafkaConsumer<T, U>,
                                      statuses: JobStatuses<T, U>,
                                      jobStatusUpdates: Map<JobId, JobStatus>)
        : JobStatuses<T, U> {

    val newJobStatues = statuses.update(jobStatusUpdates)
    val committableOffsets = newJobStatues.committableOffsets()

    if (committableOffsets.isEmpty()) {
        logger.info { "No commitable offsets" }
        return newJobStatues
    }

    logger.info { "Pushing new offsets for ${committableOffsets.count()} partitions" }
    c.commitAsync(committableOffsets, { res, exc ->
        if (exc != null) {
            logger.error(exc) { "Crashed while committing: $res"}
        }
    })
    return newJobStatues.removeCommitted(committableOffsets)
}

private fun <T, U> processJobStatuses(c: KafkaConsumer<T, U>,
                                      jobStatuses: JobStatuses<T, U>,
                                      commands: List<SetJobStatus>) =
        processSetJobStatusMessages(commands).let {
            if (it.isNotEmpty()) {
                commitFinishedJobs(c, jobStatuses, it)
            } else {
                jobStatuses
            }
        }

private fun <T, U> fetchMessagesFromKafka(c: KafkaConsumer<T, U>,
                                          outQueue: BlockingQueue<ConsumerRecord<T, U>>,
                                          jobStatuses: JobStatuses<T, U>) =
        c.poll(POLLING_INTERVAL).let {
            val newJobsStatuses = jobStatuses.addJobs(it)
            logger.info { "Adding ${it.count()} new tasks from Kafka" }
            val remainder = outQueue.offerAll(it)
            logger.debug { "Done adding tasks from remainder was ${remainder.count()}" }
            remainder to newJobsStatuses
        }

private fun <T> BlockingQueue<T>.offerAll(xs: Iterable<T>) = xs.dropWhile { offer(it) }

private fun <T, U> retryTransientFailures(outQueue: BlockingQueue<ConsumerRecord<T, U>>, jobStatuses: JobStatuses<T, U>) =
        jobStatuses.rescheduleTransientFailures().let { (newJobStatuses, producerRecs) ->
            logger.info { "Retrying ${producerRecs.count()} tasks" }
            val remainder = outQueue.offerAll(producerRecs)
            remainder to newJobStatuses
        }

private fun <T, U> processCommandQueue(
        c: KafkaConsumer<T, U>,
        jobStatuses: JobStatuses<T, U>,
        commandQueue: BlockingQueue<SetJobStatus>
) =
drainQueue(commandQueue).let {
    processJobStatuses(c, jobStatuses, it)
}

private tailrec fun <T, U> writeRemainder(
        rem: List<ConsumerRecord<T, U>>,
        c: KafkaConsumer<T, U>,
        jobStatuses: JobStatuses<T, U>,
        commandQueue: BlockingQueue<SetJobStatus>,
        outQueue: BlockingQueue<ConsumerRecord<T, U>>
        ): JobStatuses<T, U> {
    if (rem.size == 0) {
        return jobStatuses
    }
    val h = rem.first()
    val t = rem.drop(1)
    var newJobStatuses = jobStatuses
    while(!outQueue.offer(h)) {
        if(commandQueue.size == 0) {
            Thread.sleep(5)
        }
        newJobStatuses = processCommandQueue(c, jobStatuses, commandQueue)
    }
    return writeRemainder(t, c, newJobStatuses, commandQueue, outQueue)
}

private fun <T, U> createJobsFromKafka(
        c: KafkaConsumer<T, U>,
        outQueue: BlockingQueue<ConsumerRecord<T, U>>,
        commandQueue: BlockingQueue<SetJobStatus>,
        jobStatuses: JobStatuses<T, U>): JobStatuses<T, U> {
    val (remainder, newJobsStatuses) = fetchMessagesFromKafka(c, outQueue, jobStatuses)
    return writeRemainder(remainder, c, newJobsStatuses, commandQueue, outQueue)
}

private fun <T, U> createJobsFromRetries(
        c: KafkaConsumer<T, U>,
        outQueue: BlockingQueue<ConsumerRecord<T, U>>,
        commandQueue: BlockingQueue<SetJobStatus>,
        jobStatuses: JobStatuses<T, U>): JobStatuses<T, U> {
    val (remainder, newJobsStatuses) = retryTransientFailures(outQueue, jobStatuses)
    return writeRemainder(remainder, c, newJobsStatuses, commandQueue, outQueue)
}

private fun <T, U> consumerIteration(c: KafkaConsumer<T, U>,
                                     outQueue: BlockingQueue<ConsumerRecord<T, U>>,
                                     commandQueue: BlockingQueue<SetJobStatus>,
                                     oldJobStatuses: JobStatuses<T, U>): JobStatuses<T, U> =
        processCommandQueue(c, oldJobStatuses, commandQueue).let {
            createJobsFromKafka(c, outQueue, commandQueue, it)
        }.let {
            createJobsFromRetries(c, outQueue, commandQueue, it)
        }


private fun sequenceWhile(fn: () -> Boolean): Sequence<Unit> =
        object : Iterator<Unit> {
            override fun next() = Unit
            override fun hasNext(): Boolean = fn()
        }.asSequence()


private fun <T, U> consumerLoop(c: KafkaConsumer<T, U>,
                                outQueue: BlockingQueue<ConsumerRecord<T, U>>,
                                commandQueue: BlockingQueue<SetJobStatus>,
                                run: () -> Boolean) =
        sequenceWhile(run).fold(JobStatuses<T, U>()) { acc, _ ->
            consumerIteration(c, outQueue, commandQueue, acc)
        }.let {
            logger.info { "Exiting consumer loop, counts were ${it.stateCounts()}" }
        }


const val COMMAND_QUEUE_DEPTH = 1000
const val MESSAGE_QUEUE_DEPTH = 1000

class ConsumerActor<T, U>(private val kafkaConsumer: KafkaConsumer<T, U>) {
    private val outQueue = ArrayBlockingQueue<ConsumerRecord<T, U>>(MESSAGE_QUEUE_DEPTH)
    private val commandQueue = ArrayBlockingQueue<SetJobStatus>(COMMAND_QUEUE_DEPTH)
    private val runFlag = AtomicBoolean(true)
    private fun createThread() =
            Thread({ consumerLoop(kafkaConsumer, outQueue, commandQueue, runFlag::get) })
    fun start() {
        createThread().start()
    }
    fun take() = outQueue.take()
    inline fun subscribe(fn: (ConsumerRecord<T, U>) -> Unit) {
        while(true) {
            fn(take())
        }
    }
    fun stop() = runFlag.lazySet(false)
    fun setJobStatus(jobId: Pair<TopicPartition, Long>, status: JobStatus) =
            commandQueue.put(SetJobStatus(jobId, status))
}
