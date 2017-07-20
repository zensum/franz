package franz.internal

sealed class JobStatus {
    object Success : JobStatus()
    data class TransientFailure(val throwable: Throwable) : JobStatus()
    data class PermanentFailure(val throwable: Throwable) : JobStatus()
    object Incomplete : JobStatus()
    object Retry : JobStatus()
    fun isDone() = when(this) {
        is Success -> true
        is TransientFailure -> false
        is PermanentFailure -> true
        is Incomplete -> false
        is Retry -> false
    }
    fun mayRetry() = when(this) {
        is TransientFailure -> true
        else -> false
    }
}