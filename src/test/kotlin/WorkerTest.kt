import franz.JobStatus
import franz.Message
import franz.WorkerBuilder
import franz.engine.mock.MockConsumerActor
import franz.engine.mock.MockMessage
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class WorkerTest{

    private fun getTestMessage(value: String = "") = MockMessage(
        topic = "",
        value = value
    )

    @Test
    fun testEmptyWorkFlow(){
        val mockedActor = MockConsumerActor.ofString()
            WorkerBuilder.ofString
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(mockedActor.createFactory())
                .handlePiped {
                    it
                        .end()

                }
            .start()

        assertEquals(0, mockedActor.results().size)
    }

    @Test
    fun testSingleMessageMapped(){
        val mockedActor = MockConsumerActor.ofString(
            listOf(getTestMessage("dummy"))
        )

        WorkerBuilder.ofString
            .subscribedTo("TOPIC")
            .groupId("TOPIC")
            .setEngine(mockedActor.createFactory())
            .handlePiped {
                it
                    .map { it.value() }
                    .require { it == "dummy" }
                    .end()

            }
            .start()

        assertEquals(1, mockedActor.results().size)
        assertEquals(JobStatus.Success, mockedActor.results().first().status)
    }

    @Test
    fun testMultipleMessageFailed(){
        val mockedActor = MockConsumerActor.ofString(
            listOf(
                getTestMessage("dummy"),
                getTestMessage("dummy"),
                getTestMessage("dummy")
            )
        )

        WorkerBuilder.ofString
            .subscribedTo("TOPIC")
            .groupId("TOPIC")
            .setEngine(mockedActor.createFactory())
            .handlePiped {
                it
                    .map { it.value() }
                    .require { it == "not this" }
                    .end()

            }
            .start()

        assertEquals(3, mockedActor.results().size)
        assertEquals(0, mockedActor.results().filter { it.status == JobStatus.Success }.size)
    }
}