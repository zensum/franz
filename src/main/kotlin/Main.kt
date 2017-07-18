package franz
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

private fun defaultsFromEnv() = System.getenv("KAFKA_HOST").let {
 if (it == null || it.length < 1)
     emptyMap()
 else
     mapOf("bootstrap.servers" to listOf(it))
}

private val sensibleDefaults = mapOf(
        "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "group.id" to "sms-outbound",
        "enable.auto.commit" to "true",
        "auto.commit.interval.ms" to "2000",
        "acks" to "1",
        "compression.type" to "gzip",
        "timeout.ms" to "5000"
)

private fun makeConsummerConfig(userOpts: Map<String, String>) =
        sensibleDefaults + defaultsFromEnv()

private fun consumer(opts: Map<String, String>, topics: List<String>) =
        KafkaConsumer<String, String>(makeConsummerConfig(opts)).apply {
            subscribe(topics)
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

sealed class ConsumerCommand {
    data class SetJobStatus(val id: Pair<TopicPartition, Long>, val status: JobStatus) : ConsumerCommand()
    object Stop : ConsumerCommand()
}

private fun <T, U> ConsumerRecord<T, U>.topicPartition() =
        TopicPartition(topic(), partition())

private fun <T, U> ConsumerRecord<T, U>.jobId() = topicPartition() to offset()

private fun findCommitableOffsets(x: Map<Pair<TopicPartition, Long>, JobStatus>) = x
        .toList()
        .groupBy { it.first.first }
        .map { (partition, values) ->
            values.sortedBy { (key, _) -> key.second }
                    .takeWhile { (_, status) -> status.isDone() }
                    .last()?.first
        }
        .filterNotNull()
        .toMap()
        .mapValues { (_, v) -> OffsetAndMetadata(v) }

private fun <T, U> consumerLoop(c: KafkaConsumer<T, U>,
                               outQueue: BlockingQueue<ConsumerRecord<T, U>>,
                               commandQueue: BlockingQueue<ConsumerCommand>) {
    var jobStatuses = emptyMap<Pair<TopicPartition,Long>, JobStatus>()
    while (true) {
        val oldJobStatuses = jobStatuses
        while(commandQueue.isNotEmpty()) {
            val command = commandQueue.take()
            when (command) {
                is ConsumerCommand.Stop -> return
                is ConsumerCommand.SetJobStatus ->
                        jobStatuses += mapOf(command.id to command.status)
            }
        }
        val updatedJobStatues = oldJobStatuses !== jobStatuses

        if (updatedJobStatues) {
            val committableOffsets = findCommitableOffsets(jobStatuses)
            c.commitAsync(committableOffsets, { _, exc -> println(exc) })
            // Remove offsets lower than the committed ones
            // This probably shouldn't be done for commits lower than
            jobStatuses = jobStatuses.filterKeys { (topicPartition, offset) ->
                val committedOffset = committableOffsets[topicPartition]?.offset() ?: -1
                offset > committedOffset
            }
        }

        val messages = c.poll(10000)
        val newJobStatuses = messages.map { it.jobId() to JobStatus.Incomplete }.toMap()
        jobStatuses += newJobStatuses

        outQueue.addAll(messages)
    }
}

const val COMMAND_QUEUE_DEPTH = 100
const val MESSAGE_QUEUE_DEPTH = 100

class Consumer<T, U>(private val kafkaConsumer: KafkaConsumer<T, U>) {
    private val outQueue = ArrayBlockingQueue<ConsumerRecord<T, U>>(MESSAGE_QUEUE_DEPTH)
    private val commandQueue = ArrayBlockingQueue<ConsumerCommand>(COMMAND_QUEUE_DEPTH)
    private fun createThread() =
            Thread({ consumerLoop(kafkaConsumer, outQueue, commandQueue) })
    fun start() {
        createThread().start()
    }
    fun take() = outQueue.take()
    fun stop() = commandQueue.put(ConsumerCommand.Stop)
    fun setJobStatus(jobId: Pair<TopicPartition, Long>, status: JobStatus) =
            commandQueue.put(ConsumerCommand.SetJobStatus(jobId, status))
}

class JobDSL<T, U>(rec: ConsumerRecord<T, U>){
    val success = JobStatus.Success
    fun permanentFailure(ex: Throwable) = JobStatus.PermanentFailure(ex)
    fun transientFailure(ex: Throwable) = JobStatus.TransientFailure(ex)
    val key = rec.key()
    val value = rec.value()
}

private fun <T, U> tryJob(dsl: JobDSL<T, U>, fn: JobDSL<T, U>.() -> JobStatus) = try {
    fn(dsl)
} catch (exc: Exception) {
    dsl.transientFailure(exc)
}

fun <T, U> worker(cons: Consumer<T, U>, fn: JobDSL<T, U>.() -> JobStatus) {
    while (true) {
        val msg = cons.take()
        val dsl = JobDSL(msg)
        val res = tryJob(dsl, fn)
        cons.setJobStatus(msg.jobId(), res)
    }
}

fun main(args: Array<String>) {
    val c = Consumer(consumer(emptyMap(), emptyList()))
    worker(c) {
        println("Message was $value")
        success
    }
}
