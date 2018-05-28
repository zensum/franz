import franz.WorkerBuilder
import franz.engine.mock.MockConsumerActor
import org.junit.Test

class BuildTest{

    private suspend fun suspendingFunction() { "Lets do nothing" }

    @Test
    fun testMapWithSuspendFunctionWillCompile() {

        val worker =
            WorkerBuilder.ofByteArray
                .subscribedTo("TOPIC")
                .groupId("TOPIC")
                .setEngine(MockConsumerActor.ofString().createFactory())
                .handlePiped {
                    it
                        .map { suspendingFunction() }
                        .end()

                }
    }
}