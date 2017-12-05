package franz.engine.kafka_two

import com.sun.scenario.effect.Offset
import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor
import franz.engine.kafka_one.topicPartition
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.selects.whileSelect
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import kotlin.collections.HashMap
import kotlin.coroutines.experimental.CoroutineContext

internal interface Chan<T> {
    suspend fun put(msg: T) : Unit
    suspend fun take(): T
}

fun offsetActor(ctx: CoroutineContext, out: suspend (Long) -> Unit) = actor<Long>(ctx) {
    var pending = sortedSetOf<Long>()
    var done : NavigableSet<Long> = sortedSetOf()
    channel.consumeEach {
        if (it > 0) {
            val minPending = pending.first()
            val moveAhead = it == minPending
            pending.remove(it)
            done.add(it)
            if (moveAhead) {
                val newMinPending = pending.first()
                val floor = done.floor(newMinPending)
                out(floor)
                done = done.tailSet(floor, false)
            }

        } else {
            pending.add(-it)
        }
    }
}

fun offsetsActor(ctx: CoroutineContext, out: SendChannel<Pair<TopicPartition, Long>>) = actor<Pair<TopicPartition, Long>>(ctx) {
    var actors = mutableMapOf<TopicPartition, ActorJob<Long>>()
    channel.consumeEach { (tp, offset) ->
        actors.computeIfAbsent(tp) {
            offsetActor(ctx) { out.send(tp to it) }
        }.let {
            it.channel.send(offset)
        }
    }
}

internal fun <T, U> msgToOffset(m: K2Message<T, U>, delivered: Boolean) = m.offset()

fun <T, U> deliveryActor(
    ctx: CoroutineContext,
    offsets: SendChannel<Pair<TopicPartition, Long>>,
    worker: SendChannel<Message<T, U>>
) = actor<Pair<Message<T, U>, Boolean>>(ctx) {
    channel.consumeEach { (msg, completed) ->
        // Send a correct TopicPartition, Long pair
        val offset = msgToOffset(msg as K2Message<T, U>, completed)
        offsets.send(msg.topicPartition() to offset)
        // Only send non-completed messages to worker
        if (!completed) {
            worker.send(msg)
        }
    }
}


internal class K2Message<T, U>(private val rec: ConsumerRecord<T, U>) : Message<T, U> {
    override fun timestamp(): Long = rec.timestamp()
    override fun value(): U = rec.value()
    override fun key(): T = rec.key()
    override fun topic(): String = rec.topic()
    override fun headers(): Array<Pair<String, ByteArray>> = TODO("not implemented")
    override fun headers(key: String): Array<ByteArray> = TODO("not implemented")
    fun topicPartition() = rec.topicPartition()
    override fun offset() = rec.offset()

}

fun <K, V> kafkaLoop(
    ctx: CoroutineContext,
    cons: KafkaConsumer<K, V>,
    ch: SendChannel<Pair<Message<K, V>, Boolean>>,
    offsets: ReceiveChannel<Pair<TopicPartition, Long>>) = runBlocking(ctx) {

    val offsetMap = mutableMapOf<TopicPartition, OffsetAndMetadata>()
    while (true) {
        while (true) {
            val p = offsets.poll() ?: break
            offsetMap.compute(p.first) { _, v ->
                if (v == null || p.second > v.offset())
                    OffsetAndMetadata(p.second)
                else v
            }
        }
        cons.commitAsync(offsetMap.toMutableMap()) { res, exc ->
            if (exc != null) {
                // THIS IS BAD
            }
        }
        // Prepare for next round
        offsetMap.clear()

        val res = cons.poll(1000)

        val k2s = res.map {
            K2Message<K, V>(it)
        }.toList().forEach {
            ch.send(it to false)
        }
    }
}

class K2Cons <K, V>(private val ctx: CoroutineContext) {
    private val workerC = Channel<Message<K, V>>()
    private var responseC: SendChannel<Pair<Message<K, V>, Boolean>>? = null
    private val commitC = Channel<Pair<TopicPartition, Long>>(100)
    suspend fun run() {
        val offsets = offsetsActor(ctx, commitC)
        val delivery = deliveryActor(ctx, offsets.channel, workerC)
        responseC = delivery.channel
        kafkaLoop(ctx, KafkaConsumer<K, V>(Properties()), responseC!!, commitC)
    }

    fun subscribe(fn: (Message<K, V>) -> Unit) {
        async(ctx) {
            workerC.consumeEach {
                fn(it)
            }
        }
    }

    fun stop() {
        workerC?.close()
        responseC?.close()
        commitC?.close()
    }

    suspend fun done(m: Message<K, V>) {
        responseC!!.send(m to true)
    }
}


class K2ConsumerActor<T, U> : ConsumerActor<T, U> {
    private var k2: K2Cons<T, U>? = null
    private var ctx: CoroutineContext? = null

    override fun start() {
        runBlocking {
            k2 = K2Cons<T, U>(coroutineContext)
            k2!!.run()
        }
    }

    override fun stop() {
        k2!!.stop()
    }

    override fun setJobStatus(msg: Message<T, U>, status: JobStatus) {
        if (status.isDone()) {
            async(ctx!!) {
                k2!!.done(msg)
            }
        }
        // Logic for retry
    }

    override fun subscribe(fn: (Message<T, U>) -> Unit) {
        k2!!.subscribe(fn)
    }
}

