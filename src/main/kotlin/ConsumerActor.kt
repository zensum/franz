package franz.internal
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.TimeUnit


private val logger = KotlinLogging.logger {}

typealias JobId = Pair<TopicPartition, Long>
typealias SetJobStatus = Pair<JobId, JobStatus>

private suspend fun <T> drainCh(ch: ReceiveChannel<T>): List<T> =
        mutableListOf<T>()
                .also {
                    // This is not good, the
                    // item could be consumed
                    // between isEmpty and receive
                    while(!ch.isEmpty) {
                        it.add(ch.receive())
                    }
                }.toList()

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
                                          outCh: SendChannel<ConsumerRecord<T, U>>,
                                          jobStatuses: JobStatuses<T, U>) =
        if(!outCh.isClosedForSend) {
            c.poll(POLLING_INTERVAL).let {
                logger.info { "Adding ${it.count()} new tasks from Kafka" }
                outCh.offerAll(it) to jobStatuses.addJobs(it)
            }
        } else emptyList<ConsumerRecord<T, U>>() to jobStatuses


private fun <T> SendChannel<T>.offerAll(xs: Iterable<T>) = xs.dropWhile { offer(it) }

private fun <T, U> retryTransientFailures(outCh: SendChannel<ConsumerRecord<T, U>>, jobStatuses: JobStatuses<T, U>) =
        if (!outCh.isClosedForSend)
            jobStatuses.rescheduleTransientFailures().let { (newJobStatuses, producerRecs) ->
                if (producerRecs.isNotEmpty()) {
                    logger.info { "Retrying ${producerRecs.count()} tasks" }
                }
                outCh.offerAll(producerRecs) to newJobStatuses
            }
        else emptyList<ConsumerRecord<T, U>>() to jobStatuses

private suspend fun <T, U> processCommandQueue(
        c: KafkaConsumer<T, U>,
        jobStatuses: JobStatuses<T, U>,
        commandCh: ReceiveChannel<SetJobStatus>
) = drainCh(commandCh).let {
    if (it.isNotEmpty()) {
        commitFinishedJobs(c, jobStatuses.update(it.toMap()))
    } else {
        jobStatuses
    }
}

private suspend tailrec fun <T, U> writeRemainder(
        rem: List<ConsumerRecord<T, U>>,
        c: KafkaConsumer<T, U>,
        jobStatuses: JobStatuses<T, U>,
        commandCh: ReceiveChannel<SetJobStatus>,
        outCh: SendChannel<ConsumerRecord<T, U>>
        ): JobStatuses<T, U> =
    if (rem.size == 0 || outCh.isClosedForSend) {
        jobStatuses
    } else {
        val newJobStatuses = iterate({ !outCh.offer(rem.first()) }, jobStatuses) {
            if(commandCh.isEmpty) {
                delay(5, TimeUnit.MILLISECONDS)
            }
            processCommandQueue(c, it, commandCh)
        }
        writeRemainder(rem.drop(1), c, newJobStatuses, commandCh, outCh)
    }

private suspend fun <T, U> writeRemainder(prev: Pair<List<ConsumerRecord<T, U>>, JobStatuses<T, U>>,
                                  c: KafkaConsumer<T, U>,
                                  commandCh: ReceiveChannel<SetJobStatus>,
                                  outCh: SendChannel<ConsumerRecord<T, U>>) =
        writeRemainder(prev.first, c, prev.second, commandCh, outCh)

private fun sequenceWhile(fn: () -> Boolean): Sequence<Unit> =
        object : Iterator<Unit> {
            override fun next() = Unit
            override fun hasNext(): Boolean = fn()
        }.asSequence()

private inline fun <T> iterate(noinline whileFn: () -> Boolean, initialValue: T, fn: (T) -> T) =
        sequenceWhile(whileFn).fold(initialValue) { acc, _ -> fn(acc) }

private suspend fun <T, U> consumerLoop(c: KafkaConsumer<T, U>,
                                outCh: SendChannel<ConsumerRecord<T, U>>,
                                commandCh: ReceiveChannel<SetJobStatus>) =
        iterate({ !commandCh.isClosedForReceive || !outCh.isClosedForSend }, JobStatuses<T, U>()) {
            processCommandQueue(c, it, commandCh)
                    .let {
                        fetchMessagesFromKafka(c, outCh, it).let {
                            writeRemainder(it, c, commandCh, outCh)
                        }
                    }
                    .let {
                        retryTransientFailures(outCh, it).let {
                            writeRemainder(it, c, commandCh, outCh)
                        }
                    }
        }

const val COMMAND_QUEUE_DEPTH = 1000
const val MESSAGE_QUEUE_DEPTH = 1000

class ConsumerActor<T, U>(private val kafkaConsumer: KafkaConsumer<T, U>) {
    private val outCh = Channel<ConsumerRecord<T, U>>(MESSAGE_QUEUE_DEPTH)
    private val commandCh = Channel<SetJobStatus>(COMMAND_QUEUE_DEPTH)
    private fun createThread() =
            Thread({
                runBlocking {
                    consumerLoop(kafkaConsumer, outCh, commandCh)
                }
            })
    fun start() = createThread().start()
    suspend fun canTake() = !outCh.isClosedForReceive
    suspend fun take() = outCh.receive()
    fun requestStop() = outCh.close()
    fun stop() {
        requestStop()
        commandCh.close()
    }
    suspend fun setJobStatus(jobId: JobId, status: JobStatus) =
            commandCh.send(SetJobStatus(jobId, status))
}
