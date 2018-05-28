import franz.WorkerBuilder
import franz.engine.mock.MockConsumerActor
import org.junit.Test

class BuildTest{

    private suspend fun suspendMap() = "test"
    private suspend fun suspendPredicate() =  true
    private suspend fun suspendUnit(): Unit { }


    @Test
    fun testMapWithSuspendFunctionWillCompile() {

        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString().createFactory())
                .handlePiped {
                    it
                        .map { suspendMap() }
                        .require { suspendPredicate() }
                        .execute { suspendPredicate() }
                        .sideEffect { suspendUnit() }
                        .end()

                }
    }
}