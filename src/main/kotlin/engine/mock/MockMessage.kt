package franz.engine.mock

import franz.Message

class MockMessage<T>(private val topic: String, private val value: T): Message<String, T> {
    override fun offset(): Long {
        TODO("not implemented")
    }
    override fun value(): T = value
    override fun topic() = topic

    override fun headers(): Array<Pair<String, ByteArray>> {
        throw NotImplementedError()
    }

    override fun key(): String {
        throw NotImplementedError()
    }

    override fun headers(key: String): Array<ByteArray> {
        throw NotImplementedError()
    }

    override fun timestamp(): Long {
        throw NotImplementedError()
    }
}

