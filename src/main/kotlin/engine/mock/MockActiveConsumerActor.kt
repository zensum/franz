package franz.engine.mock

import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor
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

    companion object {
        fun ofByteArray(messageQueue : Queue<Message<String, ByteArray>>) =
            MockActiveConsumerActor(messageQueue)

        fun ofString(messageQueue : Queue<Message<String, ByteArray>>) =
            MockActiveConsumerActor(messageQueue)
    }
}
