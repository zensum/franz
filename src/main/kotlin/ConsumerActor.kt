package franz.internal
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

private sealed class ConsumerCommand {
    data class SetJobStatus(val id: Pair<TopicPartition, Long>, val status: JobStatus) : ConsumerCommand()
    object Stop : ConsumerCommand()
}

sealed class JobStatus {
    object Success : JobStatus()
    data class TransientFailure(val throwable: Throwable) : JobStatus()
    data class PermanentFailure(val throwable: Throwable) : JobStatus()
    object Incomplete : JobStatus()
    fun isDone() = when(this) {
        is Success -> true
        is TransientFailure -> false
        is PermanentFailure -> true
        is Incomplete -> false
    }
}

typealias JobId = Pair<TopicPartition, Long>

private fun findCommitableOffsets(x: Map<JobId, JobStatus>) = x
        .toList()
        .groupBy { it.first.first }
        .map { (_, values) ->
            values.sortedBy { (key, _) -> key.second }
                    .takeWhile { (_, status) -> status.isDone() }
                    .last().first
        }
        .filterNotNull()
        .toMap()
        .mapValues { (_, v) -> OffsetAndMetadata(v) }

private fun processSetJobStatusMessages(cmds: List<ConsumerCommand>) : Map<JobId, JobStatus> = cmds
        .filter { it is ConsumerCommand.SetJobStatus }
        .map { it as ConsumerCommand.SetJobStatus }
        .map { (id, status) -> mapOf(id to status) }
        .plusElement(emptyMap()) // reduce needs non-zero cardinality
        .reduce { a, b -> a + b }

private fun processStopCommands(cmds: List<ConsumerCommand>) = cmds.find { it is ConsumerCommand.Stop } != null

private fun <T> drainQueue(bq: BlockingQueue<T>): List<T> =
        mutableListOf<T>()
                .also { bq.drainTo(it) }
                .toList()


const val POLLING_INTERVAL = 10000L

data class JobStatuses(private val jobStatuses: Map<JobId, JobStatus> = emptyMap<JobId, JobStatus>()) {
    fun update(updates: Map<JobId, JobStatus>) = copy(jobStatuses = jobStatuses + updates)
    fun committableOffsets() = findCommitableOffsets(jobStatuses)
    fun removeCommitted(committed: Map<TopicPartition, OffsetAndMetadata>) =
            jobStatuses.filterKeys { (topicPartition, offset) ->
                val committedOffset = committed[topicPartition]?.offset() ?: -1
                offset > committedOffset
            }.let { copy(jobStatuses = it) }
    fun addJobs(jobs: Iterable<JobId>) = update(jobs.map { it to JobStatus.Incomplete }.toMap())
}

private fun <T, U> commitFinishedJobs(c: KafkaConsumer<T, U>,
                                      statuses: JobStatuses,
                                      jobStatusUpdates: Map<JobId, JobStatus>)
        : JobStatuses {

    val newJobStatues = statuses.update(jobStatusUpdates)
    val committableOffsets = newJobStatues.committableOffsets()

    if (committableOffsets.isEmpty()) {
        return statuses
    }

    c.commitAsync(committableOffsets, { _, exc ->
        if (exc != null) {
            println(exc)
        }
    })
    return newJobStatues.removeCommitted(committableOffsets)
}

private fun <T, U> processJobStatuses(c: KafkaConsumer<T, U>,
                                      jobStatuses: JobStatuses,
                                      commands: List<ConsumerCommand>) =
        processSetJobStatusMessages(commands).let {
            if (it.isNotEmpty()) {
                commitFinishedJobs(c, jobStatuses, it)
            } else {
                jobStatuses
            }
        }

private fun <T, U> fetchMessagesFromKafka(c: KafkaConsumer<T, U>,
                                          outQueue: BlockingQueue<ConsumerRecord<T, U>>,
                                          jobStatuses: JobStatuses) =
        c.poll(POLLING_INTERVAL).let {
            val newJobsStatuses = jobStatuses.addJobs(it.map { it.jobId() })
            outQueue.addAll(it)
            newJobsStatuses
        }

private fun <T, U> consumerLoop(c: KafkaConsumer<T, U>,
                                outQueue: BlockingQueue<ConsumerRecord<T, U>>,
                                commandQueue: BlockingQueue<ConsumerCommand>) {
    var jobStatuses = JobStatuses()
    var shouldStop = false
    while (!shouldStop) {
        val commands = drainQueue(commandQueue)
        shouldStop = processStopCommands(commands)
        jobStatuses =
                processJobStatuses(c, jobStatuses, commands).let {
                    fetchMessagesFromKafka(c, outQueue, it)
                }
    }
}

const val COMMAND_QUEUE_DEPTH = 100
const val MESSAGE_QUEUE_DEPTH = 100

class ConsumerActor<T, U>(private val kafkaConsumer: KafkaConsumer<T, U>) {
    private val outQueue = ArrayBlockingQueue<ConsumerRecord<T, U>>(MESSAGE_QUEUE_DEPTH)
    private val commandQueue = ArrayBlockingQueue<ConsumerCommand>(COMMAND_QUEUE_DEPTH)
    private fun createThread() =
            Thread({ consumerLoop(kafkaConsumer, outQueue, commandQueue) })
    fun start() {
        createThread().start()
    }
    fun take() = outQueue.take()
    inline fun subscribe(fn: (ConsumerRecord<T, U>) -> Unit) {
        while(true) {
            fn(take())
        }
    }
    fun stop() = commandQueue.put(ConsumerCommand.Stop)
    fun setJobStatus(jobId: Pair<TopicPartition, Long>, status: JobStatus) =
            commandQueue.put(ConsumerCommand.SetJobStatus(jobId, status))
}