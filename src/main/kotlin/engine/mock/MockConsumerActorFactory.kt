package franz.engine.mock

import franz.Message
import franz.engine.ConsumerActor
import franz.engine.ConsumerActorFactory

class MockConsumerActorFactory<TT, UU>(val consumerActor: ConsumerActor<TT, UU>) : ConsumerActorFactory {
    // Note that we're unable to enforce TT, UU being equal to T, U. Hence we do an explicit unsafe cast.
    // Note further, that because Type arguments are erased at run-time it will trust that the arguments T, U are TT, UU even at runtime
    override fun <T, U> create(opts: Map<String, Any>, topics: List<String>)  =
        consumerActor as ConsumerActor<T, U>
}

