package franz.engine.mock

import franz.Message
import java.util.*

class MockActiveConsumerActor<T, U>(val messageQueue : Queue<Message<T, U>>) : MockConsumerActorBase<T, U>() {
    override fun subscribe(fn: (Message<T, U>) -> Unit) {
        handlers.add(fn)

        while(messageQueue.isNotEmpty()){
            val message = messageQueue.poll()
            handlers.forEach { it.invoke(message) }
        }
    }

    companion object {
        fun ofByteArray(messageQueue: Queue<Message<String, ByteArray>> = createEmptyList()) =
            MockActiveConsumerActor(messageQueue)

        fun ofString(messageQueue: Queue<Message<String, ByteArray>> = createEmptyList()) =
            MockActiveConsumerActor(messageQueue)
    }
}

private fun<T, U> createEmptyList() =
    LinkedList<Message<T, U>>()
