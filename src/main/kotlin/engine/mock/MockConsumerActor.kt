package franz.engine.mock

import franz.JobStateException
import franz.JobStatus
import franz.Message
import franz.engine.kafka_one.KafkaMessage

class MockConsumerActor<T, U>(private val messages : List<Message<T, U>>) : MockConsumerActorBase<T, U>() {
    override fun subscribe(fn: (Message<T, U>) -> Unit) {
        handlers.add(fn)
        messages.forEach { m ->
            handlers.forEach { h ->
                h(m)
            }
        }
    }

    companion object {
        fun ofByteArray(messages: List<Message<String, ByteArray>> = emptyList()) =
            MockConsumerActor(messages)

        fun ofString(messages: List<Message<String, String>> = emptyList()) =
            MockConsumerActor(messages)
    }
}
