package franz

import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    val rnd = Random()
    WorkerBuilder.ofString
            .subscribedTo("my-topic")
            .groupId("test")
            .running {
                when(value) {
                    "ThisIsFine" -> if (rnd.nextBoolean()) success else transientFailure(RuntimeException("This is fine!"))
                    "ThisIsBad" -> permanentFailure(RuntimeException("It was bad"))
                    else -> success
                }
            }
            .start()

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