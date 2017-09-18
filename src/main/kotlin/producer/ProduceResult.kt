package franz.producer

interface ProduceResult {
    fun offset(): Long
    fun timestamp(): Long
    fun topic(): String
    fun partition(): Int
}