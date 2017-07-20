package franz.internal
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

typealias JobId = Pair<TopicPartition, Long>
typealias SetJobStatus = Pair<JobId, JobStatus>

private fun <T> drainQueue(bq: BlockingQueue<T>): List<T> =
        mutableListOf<T>()
                .also { bq.drainTo(it) }
                .toList()

const val POLLING_INTERVAL = 10000L

private fun <T, U> commitFinishedJobs(c: KafkaConsumer<T, U>,
                                      statuses: JobStatuses<T, U>)
        : JobStatuses<T, U> =
        statuses.committableOffsets().also {
            logger.info { "Pushing new offsets for ${it.count()} partitions" }
            c.commitAsync(it, { res, exc ->
                if (exc != null) {
                    logger.error(exc) { "Crashed while committing: $res"}
                }
            })
        }.let {
            statuses.removeCommitted(it)
        }

private fun <T, U> fetchMessagesFromKafka(c: KafkaConsumer<T, U>,
                                          outQueue: BlockingQueue<ConsumerRecord<T, U>>,
                                          jobStatuses: JobStatuses<T, U>) =
        c.poll(POLLING_INTERVAL).let {
            logger.info { "Adding ${it.count()} new tasks from Kafka" }
            outQueue.offerAll(it) to jobStatuses.addJobs(it)
        }

private fun <T> BlockingQueue<T>.offerAll(xs: Iterable<T>) = xs.dropWhile { offer(it) }

private fun <T, U> retryTransientFailures(outQueue: BlockingQueue<ConsumerRecord<T, U>>, jobStatuses: JobStatuses<T, U>) =
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
) = drainQueue(commandQueue).let {
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
            if(commandQueue.size == 0) {
                Thread.sleep(5)
            }
            processCommandQueue(c, it, commandQueue)
        }
        writeRemainder(rem.drop(1), c, newJobStatuses, commandQueue, outQueue)
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

private fun sequenceWhile(fn: () -> Boolean): Sequence<Unit> =
        object : Iterator<Unit> {
            override fun next() = Unit
            override fun hasNext(): Boolean = fn()
        }.asSequence()

private fun <T> iterate(whileFn: () -> Boolean, initialValue: T, fn: (T) -> T) =
        sequenceWhile(whileFn).fold(initialValue) { acc, _ -> fn(acc) }

private fun <T, U> consumerLoop(c: KafkaConsumer<T, U>,
                                outQueue: BlockingQueue<ConsumerRecord<T, U>>,
                                commandQueue: BlockingQueue<SetJobStatus>,
                                run: () -> Boolean) =
        iterate(run, JobStatuses<T, U>()) {
            processCommandQueue(c, it, commandQueue)
                    .let { createJobsFromKafka(c, outQueue, commandQueue, it) }
                    .let { createJobsFromRetries(c, outQueue, commandQueue, it) }
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
    fun setJobStatus(jobId: JobId, status: JobStatus) =
            commandQueue.put(SetJobStatus(jobId, status))
}
