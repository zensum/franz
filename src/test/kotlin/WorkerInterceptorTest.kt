package franz

import franz.engine.mock.MockConsumerActor
import franz.engine.mock.MockMessage
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DummyException(): Exception()

class WorkerInterceptorTest {

    fun createTestMessage(): MockMessage<String> =
        MockMessage(0, "", "this is a test message")

    @Test
    fun installNoFeature(){
        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString().createFactory())
                .handlePiped {
                    it
                        .end()

                }


        assertEquals(0, worker.getInterceptors().size)
    }

    @Test
    fun installSingleFeature(){
        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString().createFactory())
                .install(WorkerInterceptor {})
                .handlePiped {
                    it
                        .execute { true }
                        .sideEffect { println("Exeute!") }
                        .end()

                }

        assertEquals(1, worker.getInterceptors().size)

    }

    @Test
    fun installMultipleFeatures(){
        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString().createFactory())
                .install(WorkerInterceptor{})
                .install(WorkerInterceptor{})
                .install(WorkerInterceptor{})
                .handlePiped {
                    it
                        .end()

                }

        assertEquals(3, worker.getInterceptors().size)
    }

    @Test
    fun installAndRunSingleInterceptor(){
        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                .install(WorkerInterceptor{it.executeNext()})
                .handlePiped {
                    it
                        .sideEffect { println("side effect") }
                        .end()

                }

        assertEquals(1, worker.getInterceptors().size)

        worker.start()
    }

    @Test
    fun installAndRunMultipleInterceptor(){
        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                .install(WorkerInterceptor{it.executeNext()})
                .install(WorkerInterceptor{it.executeNext()})
                .handlePiped {
                    it
                        .sideEffect { println("side effect") }
                        .end()

                }

        assertEquals(2, worker.getInterceptors().size)
        worker.start()
    }

    @Test
    fun interceptorContinueExecution(){
        var setFlagged = false
        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                .install(WorkerInterceptor{it.executeNext()})
                .handlePiped {
                    it
                        .sideEffect { setFlagged = true }
                        .end()

                }
        worker.start()
        assertTrue(setFlagged)
    }

    @Test
    fun interceptorStopExecution(){
        var setFlagged = false
        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                .install(WorkerInterceptor{ /* Explicitly don't runt it.executeNext() */})
                .install(WorkerInterceptor{it.executeNext()})
                .handlePiped {
                    it
                        .sideEffect { setFlagged = true }
                        .end()

                }
        worker.start()
        assertFalse(setFlagged)
    }

    @Test
    fun throwsUnhandled(){

        // The Franz worker should swallow this exception and return a transient failure
        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                .handlePiped {
                    it
                        .sideEffect { println("Side effect") }
                        .end()

                }

        worker.start()
    }

    @Test
    fun tryCatchInInterceptor(){
        var exceptionEncountered = false

        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                .install(WorkerInterceptor {
                    try{
                        it.executeNext()
                    }catch (e: DummyException){
                        exceptionEncountered = true
                    }
                })
                .handlePiped {
                    it
                        .sideEffect { throw DummyException() }
                        .end()

                }

        worker.start()
        assertTrue(exceptionEncountered)
    }
}