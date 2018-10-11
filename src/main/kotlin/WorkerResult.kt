package franz

class WorkerResult<out L> (
    val value: L? = null,
    val status: WorkerStatus
) {
    companion object {
        fun <L> success(result: L) = WorkerResult(result, WorkerStatus.Success)
        val retry = WorkerResult(null, WorkerStatus.Retry)
        val failure = WorkerResult(null, WorkerStatus.Failure)
    }

    fun toJobStatus(): JobStatus = when(status){
        WorkerStatus.Success -> JobStatus.Incomplete
        WorkerStatus.Retry -> JobStatus.TransientFailure
        WorkerStatus.Failure -> JobStatus.PermanentFailure
    }
}

enum class WorkerStatus {
    Success,
    Retry,
    Failure
}