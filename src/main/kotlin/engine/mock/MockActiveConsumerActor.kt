package franz.engine.mock

import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor
import java.util.*

class MockActiveConsumerActor<T, U>(val messageQueue : Queue<Message<T, U>>) : ConsumerActor<T, U> {
    var results = mutableMapOf<Message<T, U>, JobStatus>()
    private var handlers = mutableListOf<(Message<T, U>) -> Unit>()
    private val workerThread = Thread {
        try {
            while (true) {
                val m = messageQueue.poll()
                handlers.forEach { h ->
                    h(m)
                }
            }
        }catch (e: InterruptedException){
            // When interrupted, quit this thread
        }
    }
    override fun start() {
        workerThread.start()
    }

    override fun stop() {
        workerThread.interrupt()
    }

    override fun setJobStatus(msg: Message<T, U>, status: JobStatus) {
        results[msg] = status
    }

    override fun subscribe(fn: (Message<T, U>) -> Unit) {
        handlers.add(fn)
    }

    companion object {
        fun ofByteArray(messageQueue : Queue<Message<String, ByteArray>>) =
            MockActiveConsumerActor(messageQueue)

        fun ofString(messageQueue : Queue<Message<String, ByteArray>>) =
            MockActiveConsumerActor(messageQueue)
    }
}
