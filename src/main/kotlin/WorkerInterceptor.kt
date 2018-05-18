package franz

class WorkerInterceptor(
    var next: WorkerInterceptor? = null,
    val onIntercept: ((WorkerInterceptor) -> Unit) = {
        it.executeNext()
    }
){
    fun executeNext(){
        val nextInterceptor = next
        if(nextInterceptor != null){
            nextInterceptor.onIntercept(nextInterceptor)
        }
    }
}