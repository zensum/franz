package franz.engine.kafka_one

import franz.engine.ConsumerActor
import franz.engine.ConsumerActorFactory

object KafkaConsumerActorFactory : ConsumerActorFactory {
    override fun <T, U> create(opts: Map<String, Any>, topics: List<String>): ConsumerActor<T, U> =
        KafkaConsumerActor(opts, topics)
}
