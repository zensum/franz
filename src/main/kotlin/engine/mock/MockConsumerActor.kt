package franz.engine.mock

import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor

class MockConsumerActor<T, U>(private val messages : List<Message<T, U>>) : ConsumerActor<T, U> {
    private var handlers = mutableListOf<(Message<T, U>) -> Unit>()
    var results = mutableMapOf<Message<T, U>, JobStatus>()

    override fun start() {
        Thread {
            messages.forEach { m ->
                println("Found message")
                handlers.forEach { h ->
                    println("Handled message")
                    h(m)
                }
            }
        }.start()
    }

    override fun stop() {
    }

    override fun setJobStatus(msg: Message<T, U>, status: JobStatus) {
       results[msg] = status
    }

    override fun subscribe(fn: (Message<T, U>) -> Unit) {
        handlers.add(fn)
    }

    fun createFactory() =
        MockConsumerActorFactory(this)

    companion object {
        fun ofByteArray(messages: List<Message<String, ByteArray>>) =
            MockConsumerActor(messages)

        fun ofString(messages: List<Message<String, String>>) =
            MockConsumerActor(messages)
    }
}
