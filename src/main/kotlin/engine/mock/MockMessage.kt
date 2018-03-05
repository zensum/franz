package franz.engine.mock

import franz.Message
import java.time.Instant

class MockMessage<T>(
    private val offset: Long = 1L,
    private val topic: String,
    private val value: T,
    private val headers: Array<Pair<String, ByteArray>> = emptyArray(),
    private val key: String = "key:$offset",
    private val timestamp: Long = Instant.now().epochSecond
): Message<String, T> {
    override fun offset(): Long = offset
    override fun value(): T = value
    override fun topic() = topic

    override fun headers(): Array<Pair<String, ByteArray>> = headers

    override fun key(): String = key

    override fun headers(key: String): Array<ByteArray> = headers()
    .filter { it.first == key }
    .map { it.second }
    .toTypedArray()

    override fun timestamp(): Long = timestamp
}

