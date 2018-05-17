package franz

import franz.engine.mock.MockConsumerActor
import franz.engine.mock.MockMessage
import org.junit.jupiter.api.Test
import kotlin.test.*

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
                .install(WorkerInterceptor{println("Test")})
                .handlePiped {
                    it
                        .sideEffect { println("sideeffect") }
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
                .install(WorkerInterceptor{println("Test 01")})
                .install(WorkerInterceptor{println("Test 02")})
                .handlePiped {
                    it
                        .sideEffect { println("sideeffect") }
                        .end()

                }

        assertEquals(2, worker.getInterceptors().size)
        worker.start()
    }
}