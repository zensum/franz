package franz.engine.mock

import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor

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

    fun createFactory() =
        MockConsumerActorFactory(this)

    companion object {
        fun ofByteArray(messages: List<Message<String, ByteArray>> = emptyList()) =
            MockConsumerActor(messages)

        fun ofString(messages: List<Message<String, String>> = emptyList()) =
            MockConsumerActor(messages)
    }
}
