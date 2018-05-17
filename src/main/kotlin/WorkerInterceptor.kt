package franz

class WorkerInterceptor(
    private val previous: WorkerInterceptor? = null,
    val onIntercept: ((WorkerInterceptor) -> Unit) = {}
){
    fun <U> execute(fn: (U) -> Boolean, value: U){
        println("Execute")

        this.onIntercept(this)

        if(previous != null){
            previous.execute(fn, value)
        }
    }
}