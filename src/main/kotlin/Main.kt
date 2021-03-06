package franz

import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.*

val logger = KotlinLogging.logger("test-logger")

suspend fun main(args: Array<String>) {
    val rnd = Random()
    WorkerBuilder.ofString
        .subscribedTo("my-topic")
        .groupId("test")
        .handler {
                when (it.value()) {
                    "ThisIsFine" -> if (rnd.nextBoolean()) JobStatus.Success else JobStatus.TransientFailure
                    "ThisIsBad" -> JobStatus.PermanentFailure
                    else -> JobStatus.Success
                }
            }
            .start()

    WorkerBuilder
        .ofString
        .subscribedTo("my-topic")
        .groupId("test2")
        .handlePiped {
            it
                .require("It can't be bad") { it.value() != "ThisIsBad" }
                .execute("It doesn't always work") { it.value() == "ThisIsFine" && rnd.nextBoolean() }
                .end()
        }

    runBlocking {
        val myTopic = ProducerBuilder.ofString.create().forTopic("my-topic")
        while (true) {
            myTopic.send("ThisIsFine")
            myTopic.send("ThisIsBad")
            myTopic.send("ThisIsGood")
            Thread.sleep(2000)
        }
    }
}