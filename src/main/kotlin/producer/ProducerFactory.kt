package franz.producer

interface ProducerFactory {
    fun <T, U> create(opts: Map<String, Any>): Producer<T, U>
}