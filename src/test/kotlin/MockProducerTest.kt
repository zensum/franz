import franz.*
import franz.engine.mock.MockConsumerActor
import franz.engine.mock.MockMessage
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals

typealias PipedWorkerFunction<T, U> = suspend (JobState<Message<T, U>>) -> JobStatus

class MockProducerTest {

    private suspend fun getMockWorker(actor: MockConsumerActor<String, String>, fn: PipedWorkerFunction<String, String>) = WorkerBuilder.ofString
        .subscribedTo("dummy")
        .groupId("dummy")
        .setEngine(actor.createFactory())
        .handlePiped(fn)

    private fun getMockMessage(message: String) =
        MockMessage(topic = "dummy", value = message)

    @Test
    fun testMapSuccess(){
        runBlocking {
            val mockConsumer = MockConsumerActor.ofString(listOf(getMockMessage("hai")))

            getMockWorker(mockConsumer) {
                JobState("value", Stack(), Stack(), emptyList())
                    .map { true }
                    .end()
            }.start()

            val result = mockConsumer.results().first()

            assertEquals(JobStatus.Success, result.status)
        }
    }

    @Test
    fun testMapThrows(){
        runBlocking {
            val mockConsumer = MockConsumerActor.ofString(listOf(getMockMessage("hai")))

            getMockWorker(mockConsumer) {
                JobState("value", Stack(), Stack(), emptyList())
                    .sideEffect{ println("Im running")}
                    .map { throw DummyException() }
                    .end()
            }.start()

            val result = mockConsumer.results().first()

            assertEquals(JobStatus.TransientFailure, result.status)
        }
    }

    @Test
    fun testMapRequireSuccess(){
        runBlocking {
            val mockConsumer = MockConsumerActor.ofString(listOf(getMockMessage("hai")))

            getMockWorker(mockConsumer) {
                JobState("value", Stack(), Stack(), emptyList())
                    .mapRequire { true }
                    .end()
            }.start()

            val result = mockConsumer.results().first()

            assertEquals(JobStatus.Success, result.status)
        }
    }

    @Test
    fun testMapRequireThrows(){
        runBlocking {
            val mockConsumer = MockConsumerActor.ofString(listOf(getMockMessage("hai")))

            getMockWorker(mockConsumer) {
                JobState("value", Stack(), Stack(), emptyList())
                    .mapRequire { throw DummyException() }
                    .end()
            }.start()

            val result = mockConsumer.results().first()

            assertEquals(JobStatus.PermanentFailure, result.status)
        }
    }
}