package franz

import franz.engine.mock.MockConsumerActor
import franz.engine.mock.MockMessage
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DummyException(): Exception()

class WorkerInterceptorTest {

    fun createTestMessage(): MockMessage<String> =
        MockMessage(0, "", "this is a test message")

    @Test
    fun installNoFeature(){
        runBlocking {
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
    }

    @Test
    fun installSingleFeature(){
        runBlocking {
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString().createFactory())
                    .install(WorkerInterceptor())
                    .handlePiped {
                        it
                            .execute { true }
                            .sideEffect { }
                            .end()

                    }

            assertEquals(1, worker.getInterceptors().size)
        }
    }

    @Test
    fun installMultipleFeatures(){
        runBlocking {
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString().createFactory())
                    .install(WorkerInterceptor())
                    .install(WorkerInterceptor())
                    .install(WorkerInterceptor())
                    .handlePiped {
                        it
                            .end()

                    }

            assertEquals(3, worker.getInterceptors().size)
        }
    }

    @Test
    fun installInterceptor(){
        runBlocking {
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                    .install(WorkerInterceptor { i, d -> i.executeNext(d) })
                    .handlePiped {
                        it
                            .sideEffect { }
                            .end()

                    }

            assertEquals(1, worker.getInterceptors().size)
        }
    }

    @Test
    fun installMultipleInterceptor(){
        runBlocking {
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                    .install(WorkerInterceptor { i, d -> i.executeNext(d) })
                    .install(WorkerInterceptor { i, d -> i.executeNext(d) })
                    .handlePiped {
                        it
                            .sideEffect { }
                            .end()

                    }

            assertEquals(2, worker.getInterceptors().size)
        }
    }

    @Test
    fun interceptorContinueExecution(){
        runBlocking {
            var setFlagged = false
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                    .install(WorkerInterceptor { i, d -> i.executeNext(d) })
                    .handlePiped {
                        it
                            .sideEffect { setFlagged = true }
                            .end()

                    }
            worker.start()
            assertTrue(setFlagged)
        }
    }

    @Test
    fun interceptorStopExecution(){
        runBlocking {
            var setFlagged = false
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                    .install(WorkerInterceptor()) /* Explicitly don't runt it.executeNext() */
                    .install(WorkerInterceptor { i, d -> i.executeNext(d) })
                    .handlePiped {
                        it
                            .sideEffect { setFlagged = true }
                            .end()

                    }
            worker.start()
            assertFalse(setFlagged)
        }
    }

    @Test
    fun throwsUnhandled(){
        runBlocking {
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                    .handlePiped {
                        it
                            .sideEffect { }
                            .end()

                    }

            worker.start()
        }
    }

    @Test
    fun tryCatchInInterceptorWithRequire(){
        runBlocking {
            var exceptionEncountered = false
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                    .install(WorkerInterceptor { i, default ->
                        try {
                            i.executeNext(default)
                        } catch (e: DummyException) {
                            exceptionEncountered = true
                        }
                        JobStatus.PermanentFailure
                    })
                    .handlePiped {
                        it
                            .require { throw DummyException() }
                            .end()

                    }

            worker.start()
            assertTrue(exceptionEncountered)
        }
    }

    @Test
    fun tryCatchInInterceptorWithMap(){
        runBlocking {
            var exceptionEncountered = false

            val engine = MockConsumerActor.ofString(listOf(createTestMessage()))
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(engine.createFactory())
                    .install(WorkerInterceptor { i, default ->
                        try {
                            i.executeNext(default)
                        } catch (e: Exception) {
                            exceptionEncountered = true
                        }
                        JobStatus.PermanentFailure
                    })
                    .handlePiped {
                        it
                            .map { throw DummyException() }
                            .end()

                    }

            worker.start()
            assertTrue(exceptionEncountered)
        }
    }

    @Test
    fun runSingleInterceptorSeveralStages(){
        runBlocking {
            var count = 0

            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                    .install(WorkerInterceptor { _, _ ->
                        count++
                        JobStatus.Success
                    })
                    .handlePiped {
                        it
                            .execute { true }
                            .execute { true }
                            .sideEffect { }
                            .end()

                    }

            worker.start()

            // One interceptor, three job stages, one message
            assertEquals(3, count)
        }
    }

    @Test
    fun runSingleInterceptorSeveralMessages(){
        runBlocking {
            var count = 0

            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(
                        createTestMessage(),
                        createTestMessage(),
                        createTestMessage(),
                        createTestMessage()
                    )).createFactory())
                    .install(WorkerInterceptor { _, _ ->
                        count++
                        JobStatus.Success
                    })
                    .handlePiped {
                        it
                            .sideEffect { }
                            .end()

                    }

            worker.start()

            // One interceptor, one job stages, four messages
            assertEquals(4, count)
        }
    }

    @Test
    fun overrideJobStatus(){
        runBlocking {
            val mockConsumerActor = MockConsumerActor.ofString(listOf(createTestMessage()))
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(mockConsumerActor.createFactory())
                    .install(WorkerInterceptor { i, d ->
                        i.executeNext(d)
                        JobStatus.Success
                    })
                    .handlePiped {
                        it
                            .execute { false }
                            .end()

                    }

            worker.start()

            val result = mockConsumerActor.results().first()

            // As the execute block returns false, this should result in an error. However, the interceptor override the result making it a success
            assertEquals(JobStatus.Success, result.status)
        }
    }

    @Test
    fun overrideMultipleJobStatus(){
        runBlocking {
            val mockConsumerActor = MockConsumerActor.ofString(listOf(createTestMessage()))
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(mockConsumerActor.createFactory())
                    .install(WorkerInterceptor { i, d ->
                        i.executeNext(d)
                        JobStatus.Success
                    })
                    .handlePiped {
                        it
                            .execute { true }
                            .execute { false }
                            .end()

                    }

            worker.start()

            val result = mockConsumerActor.results().first()

            // As the execute block returns false, this should result in an error. However, the interceptor override the result making it a success
            assertEquals(JobStatus.Success, result.status)
        }
    }

    @Test
    fun overrideMultipleInterceptors(){
        runBlocking {
            val mockConsumerActor = MockConsumerActor.ofString(listOf(createTestMessage()))
            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(mockConsumerActor.createFactory())
                    .install(WorkerInterceptor { i, d ->
                        i.executeNext(d)
                        JobStatus.PermanentFailure
                    })
                    .install(WorkerInterceptor { i, d ->
                        i.executeNext(d)
                        JobStatus.TransientFailure
                    })
                    .handlePiped {
                        it
                            .execute { true }
                            .execute { false }
                            .end()

                    }

            worker.start()

            val result = mockConsumerActor.results().first()

            // The last interceptor returns a permanent failure, so that should be the final result of the worker
            assertEquals(JobStatus.PermanentFailure, result.status)
        }
    }

    @Test
    fun mapJobState(){
        runBlocking {
            var count = 0

            val worker =
                WorkerBuilder.ofByteArray
                    .subscribedTo("TOPIC")
                    .groupId("TOPIC")
                    .setEngine(MockConsumerActor.ofString(listOf(createTestMessage())).createFactory())
                    .install(WorkerInterceptor { i, d ->
                        count++
                        i.executeNext(d)
                    })
                    .handlePiped {
                        it
                            .execute { true }
                            .map { it.key() }
                            .execute { false }
                            .require { false }
                            .end()

                    }

            worker.start()
            assertEquals(4, count)
        }
    }
}