package franz.engine.mock

import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor
import franz.engine.WorkerFunction
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import sun.management.snmp.jvminstr.JvmThreadInstanceEntryImpl.ThreadStateMap.Byte0.runnable

class MockConsumerActor<T, U>(private val messages : List<Message<T, U>>) : ConsumerActor<T, U> {
    private var handlers = mutableListOf<(Message<T, U>) -> Unit>()
    var results = mutableMapOf<Message<T, U>, JobStatus>()

    override fun start() = Unit
    override fun stop() = Unit

    override fun setJobStatus(msg: Message<T, U>, status: JobStatus) {
       results[msg] = status
    }

    override fun subscribe(fn: (Message<T, U>) -> Unit) {
        handlers.add(fn)

        messages.forEach { m ->
            handlers.forEach { h ->
                h(m)
            }
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

    fun createFactory() =
        MockConsumerActorFactory(this)

    companion object {
        fun ofByteArray(messages: List<Message<String, ByteArray>> = emptyList()) =
            MockConsumerActor(messages)

        fun ofString(messages: List<Message<String, String>> = emptyList()) =
            MockConsumerActor(messages)
    }
}
