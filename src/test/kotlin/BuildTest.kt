import franz.WorkerBuilder
import franz.engine.mock.MockConsumerActor
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test

class BuildTest{

    private suspend fun suspendMap() = "test"
    private suspend fun suspendPredicate() =  true
    private suspend fun suspendUnit(): Unit { }
    private fun nonSuspendingFunction(): Boolean = true

    @Test
    fun testMapWithSuspendFunctionWillCompile() {
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
                    .end { suspendPredicate() }

            }
    }

    @Test
    fun testMapWithNonSuspendFunctionWillCompile() {
        WorkerBuilder.ofByteArray
            .subscribedTo("TOPIC")
            .groupId("TOPIC")
            .setEngine(MockConsumerActor.ofString().createFactory())
            .handlePiped {
                it
                    .advanceIf { nonSuspendingFunction() }
                    .execute { nonSuspendingFunction() }
                    .require { nonSuspendingFunction() }
                    .sideEffect { nonSuspendingFunction() }
                    .map { nonSuspendingFunction() }
                    .end { nonSuspendingFunction() }
            }
    }
}