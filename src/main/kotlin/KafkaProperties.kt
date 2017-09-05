package franz.internal

private fun defaultsFromEnv() = System.getenv("KAFKA_HOST").let {
    if (it.isNullOrEmpty())
        emptyMap()
    else
        mapOf("bootstrap.servers" to listOf(it))
}

internal fun createKafkaConfig(userOpts: Map<String, Any>, defaults: Map<String, Any>) =
        defaults + defaultsFromEnv() + userOpts
