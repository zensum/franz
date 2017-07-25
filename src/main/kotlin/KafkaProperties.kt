package franz.internal

private fun defaultsFromEnv() = System.getenv("KAFKA_HOST").let {
    if (it == null || it.length < 1)
        emptyMap()
    else
        mapOf("bootstrap.servers" to listOf(it))
}

internal fun createKafkaConfig(userOpts: Map<String, Any>, defaults: Map<String, Any>) =
        defaults + defaultsFromEnv() + userOpts
