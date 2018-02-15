package franz.producer

interface ProducerFactory<T, U> {
    fun create(opts: Map<String, Any>): Producer<T, U>
}