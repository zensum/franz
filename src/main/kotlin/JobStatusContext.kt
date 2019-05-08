package franz


data class JobStatusContext(
    val workerStepName: String?,
    val jobStatus: JobStatus,
    val input: Any?
)