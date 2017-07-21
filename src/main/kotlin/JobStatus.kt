package franz.internal

enum class JobStatus {
    Success,
    TransientFailure,
    PermanentFailure,
    Incomplete,
    Retry;

    fun isDone() = when(this) {
        Success -> true
        TransientFailure -> false
        PermanentFailure -> true
        Incomplete -> false
        Retry -> false
    }
    fun mayRetry() = this == TransientFailure
}