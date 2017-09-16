package franz

interface ConsumerActorFactory {
    fun <T, U> create(opts: Map<String, Any>, topics: List<String>): ConsumerActor<T, U>
}