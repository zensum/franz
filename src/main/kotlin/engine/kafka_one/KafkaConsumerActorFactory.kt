package franz.engine.kafka_one

import franz.ConsumerActor
import franz.ConsumerActorFactory

object KafkaConsumerActorFactory : ConsumerActorFactory {
    override fun <T, U> create(opts: Map<String, Any>, topics: List<String>): ConsumerActor<T, U> =
        KafkaConsumerActor(opts, topics)
}
