import franz.*
import franz.engine.mock.MockConsumerActor
import franz.engine.mock.MockMessage
import kotlinx.coroutines.experimental.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class WorkerTest{

    private fun getTestMessage(value: String = "") = MockMessage(
        topic = "",
        value = value
    )

    @Test
    fun testEmptyWorkFlow(){
        runBlocking {
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
    }

    @Test
    fun testSingleMessageMapped(){
        runBlocking {
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
    }

    @Test
    fun testMultipleMessageFailed(){
        runBlocking {
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

    @Test
    fun testMultipleAllJobStates(){
        runBlocking {
            val mockedActor = MockConsumerActor.ofString(
                listOf(
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
                        .require { true }
                        .execute { true }
                        .sideEffect { }
                        .end()

                }
                .start()

            assertEquals(JobStatus.Success, mockedActor.results().first().status)
        }
    }

    @Test
    fun testExecuteToResultSuccess(){
        runBlocking {
            val mockedActor = MockConsumerActor.ofString(
                listOf(
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
                        .executeToResult{
                            WorkerResult.success("test")
                        }
                        .require { it == "test" }
                        .end()

                }
                .start()

            assertEquals(JobStatus.Success, mockedActor.results().first().status)
        }
    }

    @Test
    fun testExecuteToResultRetry(){
        runBlocking {
            val mockedActor = MockConsumerActor.ofString(
                listOf(
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
                        .executeToResult{
                            WorkerResult.retry
                        }
                        .require { it == "test" }   // This should not matter as the earlier worker ended with retry
                        .end()

                }
                .start()

            assertEquals(JobStatus.TransientFailure, mockedActor.results().first().status)
        }
    }

    @Test
    fun testExecuteToResult(){
        runBlocking {
            val mockedActor = MockConsumerActor.ofString(
                listOf(
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
                        .executeToResult{
                            if(true) {
                                WorkerResult.success(100)
                            }else{
                                WorkerResult.retry
                            }
                        }
                        .require { it == 100 }   // This should not matter as the earlier worker ended with retry
                        .end()

                }
                .start()

            assertEquals(JobStatus.Success, mockedActor.results().first().status)
        }
    }
}