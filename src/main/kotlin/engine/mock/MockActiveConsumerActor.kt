package franz.engine.mock

import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor
import franz.engine.WorkerFunction
import kotlinx.coroutines.experimental.runBlocking
import java.util.*

class MockActiveConsumerActor<T, U>(val messageQueue : Queue<Message<T, U>>) : ConsumerActor<T, U> {
    var results = mutableMapOf<Message<T, U>, JobStatus>()
    private var handlers = mutableListOf<(Message<T, U>) -> Unit>()

    override fun start() = Unit
    override fun stop() = Unit

    override fun setJobStatus(msg: Message<T, U>, status: JobStatus) {
        results[msg] = status
    }

    override fun subscribe(fn: (Message<T, U>) -> Unit) {
        handlers.add(fn)

        while(messageQueue.isNotEmpty()){
            val message = messageQueue.poll()
            handlers.forEach { it.invoke(message) }
        }
    }

    override fun createWorker(fn: WorkerFunction<T, U>): Runnable {
        val consumer = this
        return object : Runnable {
            override fun run() {
                worker(consumer, fn)
            }
        }
    }

    private inline fun tryJobStatus(fn: () -> JobStatus) = try {
        fn()
    } catch (ex: Exception) {
        JobStatus.TransientFailure
    }

    private fun <T, U> worker(consumer: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = consumer.subscribe {
        consumer.setJobStatus(it, tryJobStatus {
            runBlocking { fn(it) }
        })
    }
    companion object {
        fun ofByteArray(messageQueue : Queue<Message<String, ByteArray>>) =
            MockActiveConsumerActor(messageQueue)

        fun ofString(messageQueue : Queue<Message<String, ByteArray>>) =
            MockActiveConsumerActor(messageQueue)
    }
}
