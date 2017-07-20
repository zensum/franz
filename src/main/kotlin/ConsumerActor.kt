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
data class SetJobStatus(val id: JobId, val status: JobStatus)

private fun processSetJobStatusMessages(cmds: List<SetJobStatus>) : Map<JobId, JobStatus> =
        cmds.map { (id, status) -> id to status }.toMap()

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
            logger.info { "Adding ${it.count()} new tasks from Kafka" }
            outQueue.offerAll(it) to jobStatuses.addJobs(it)
        }.also {
            if (it.first.count() > 0) {
                logger.debug { "Done adding tasks from remainder was ${it.first.count()}" }
            }
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
) =
        processJobStatuses(c, jobStatuses, drainQueue(commandQueue))

private fun <T, U> readUntilWritten(toWrite: ConsumerRecord<T, U>,
                                    dest: BlockingQueue<ConsumerRecord<T, U>>,
                                    commandQueue: BlockingQueue<SetJobStatus>,
                                    jobStatuses: JobStatuses<T, U>,
                                    c: KafkaConsumer<T, U>) =
        sequenceWhile { !dest.offer(toWrite) }.fold(jobStatuses) { acc, _ ->
            if(commandQueue.size == 0) {
                Thread.sleep(5)
            }
            processCommandQueue(c, acc, commandQueue)
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
        val newJobStatuses = readUntilWritten(rem.first(), outQueue, commandQueue, jobStatuses, c)
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
    fun setJobStatus(jobId: JobId, status: JobStatus) =
            commandQueue.put(SetJobStatus(jobId, status))
}
