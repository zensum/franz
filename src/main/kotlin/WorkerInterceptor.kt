package franz

typealias InterceptorStage = suspend(WorkerInterceptor, JobStatus) -> JobStatus

open class WorkerInterceptor(
    var jobState: JobState<Any>? = null,
    val next: WorkerInterceptor? = null,
    val onIntercept: InterceptorStage = { interceptor, default ->
        interceptor.executeNext(default)
    }
){
    suspend fun executeNext(default: JobStatus): JobStatus {
        val nextInterceptor: WorkerInterceptor = next ?: return JobStatus.Success
        return nextInterceptor.onIntercept.invoke(nextInterceptor, default)
    }

    fun <U: Any> setState(state: JobState<U>) {
        this.jobState = state as? JobState<Any>
        next?.setState(state)
    }

    open fun size(): Int = 1 + (next?.size() ?: 0)
}

fun WorkerInterceptor?.addInterceptor(next: WorkerInterceptor): WorkerInterceptor =
    this?.next?.addInterceptor(next) ?: next

internal object EmptyInterceptor: WorkerInterceptor(null, null, { _, _ -> JobStatus.Incomplete }) {
    fun addInterceptor(next: WorkerInterceptor): WorkerInterceptor = next

    override fun size(): Int = 0
}