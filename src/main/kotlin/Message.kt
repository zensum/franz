package franz

interface Message<out K, out V> {
    fun value() : V
    fun headers(): Array<Pair<String, ByteArray>>
    fun headers(key: String): Array<ByteArray>
    fun key(): K
    fun topic(): String
}
