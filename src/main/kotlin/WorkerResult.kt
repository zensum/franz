package franz

class WorkerResult<out L> (
    val value: L? = null,
    val status: WorkerResultStatus
) {
    companion object {
        fun <L> result(result: L) = WorkerResult(result, WorkerResultStatus.Success)
        val retry = WorkerResult(null, WorkerResultStatus.Retry)
        val failure = WorkerResult(null, WorkerResultStatus.Failure)
    }

    fun toJobStatus(): JobStatus = when(status){
        WorkerResultStatus.Success -> JobStatus.Incomplete
        WorkerResultStatus.Retry -> JobStatus.TransientFailure
        WorkerResultStatus.Failure -> JobStatus.PermanentFailure
    }
}

enum class WorkerResultStatus {
    Success,
    Retry,
    Failure
}