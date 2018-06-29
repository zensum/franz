package franz

import kotlinx.coroutines.experimental.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.*

class TestMessage<T>(private val value: T) : Message<String, T> {
    override fun offset(): Long = 0
    override fun value(): T = value
    override fun headers(): Array<Pair<String, ByteArray>> {
        throw NotImplementedError()
    }

    override fun key(): String {
        throw NotImplementedError()
    }

    override fun headers(key: String): Array<ByteArray> {
        throw NotImplementedError()
    }

    override fun topic(): String {
        throw NotImplementedError()
    }

    override fun timestamp(): Long {
        throw NotImplementedError()
    }
}

class JobStateTest {
    private fun <U> jobFrom(value: U): JobState<Message<String, U>> = JobState(TestMessage(value))
    val jobOne = jobFrom("1")

    @Test
    fun testCreateJobState() {
        val job = jobOne
        assertEquals("1", job.value!!.value())
    }

    @Test
    fun testValidateTrue() {
        val job = jobOne
        val status = runBlocking {
            job
                .require { true }
                .require { true }
        }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testValidateFalse() {
        val job = jobOne
        val status = runBlocking {
            job
                .require { true }
                .require { false }
                .require { true }
        }

        assertEquals(JobStatus.PermanentFailure, status.status)
    }

    @Test
    fun testExecuteTrue() {
        val job = jobOne
        val status = runBlocking {
            job
                .execute { true }
                .execute { true }
        }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testExecuteFalse() {
        val job = jobOne
        val status = runBlocking {
            job
                .execute { true }
                .execute { false }
                .execute { true }
        }

        assertEquals(JobStatus.TransientFailure, status.status)
    }

    @Test
    fun testConfirmTrue() {
        val job = jobOne
        val status = runBlocking {
            job
                .advanceIf { true }
                .advanceIf { true }
        }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testConfirmFalse() {
        val job = jobOne
        val status = runBlocking {
            job
                .advanceIf { true }
                .advanceIf { false }
                .advanceIf { true }
        }

        assertEquals(JobStatus.Success, status.status)
    }

    @Test
    fun testMapSuccessful() {
        val job = jobOne
        val status = runBlocking {
            job
                .require { true }
                .map { it.value() }
                .map(Integer::parseInt)
                .require { it == 1 }
        }

        assertEquals(1, status.value)
    }

    @Test
    fun testMapToNull() {
        val job = jobOne

        val state = runBlocking {
            job
                .require { true }
                .require { false }
                .map { it.value() }
                .map(Integer::parseInt)
        }

        assertNull(state.value)
    }

    @Test
    fun testMapThrows() {
        val job = jobOne

        val state = runBlocking {
            job
                .map { throw Exception("") }
        }

        assertEquals(JobStatus.TransientFailure, state.status)
    }

    @Test
    fun testMapRequireSuccessful() {
        val job = jobOne
        val status = runBlocking {
            job
                .require { true }
                .mapRequire { it.value() }
                .mapRequire(Integer::parseInt)
                .require { it == 1 }
        }

        assertEquals(1, status.value)
    }

    @Test
    fun testMapRequireToNull() {
        val job = jobOne

        val state = runBlocking {
            job
                .require { true }
                .require { false }
                .mapRequire { it.value() }
                .mapRequire(Integer::parseInt)
        }

        assertNull(state.value)
    }

    @Test
    fun testMapRequireThrows() {
        val job = jobOne

        val state = runBlocking {
            job
                .mapRequire { throw Exception("") }
        }

        assertEquals(JobStatus.PermanentFailure, state.status)
    }

    @Test
    fun testEndSuccessful() {
        val job = jobOne

        val status = runBlocking {
            job
                .require { true }
                .execute { true }
                .advanceIf { true }
                .end { true }
        }

        assertEquals(JobStatus.Success, status)
    }

    @Test
    fun testEndFailure() {
        val job = jobOne

        val status = runBlocking {
            job
                .require { true }
                .execute { true }
                .advanceIf { true }
                .end { false }
        }

        assertEquals(JobStatus.TransientFailure, status)
    }

    @Test
    fun testConversionWithTwoMapsInSequence() {
        val job = jobOne
        val result = runBlocking {
            job
                .advanceIf { it.value().isNotEmpty() }
                .map { it.value() }
                .map(Integer::parseInt)
                .map { it * 2 }
                .map { it + 4 }
                .require { it > 1 }
                .end { it > 0 }
        }

        assertEquals(JobStatus.Success, result)
    }

    @Test
    fun testConversionWithFailingValidationAndMap() {
        val job = jobOne
        val result = runBlocking {
            job
                .map { it.value() }
                .advanceIf { it.isNotEmpty() }
                .map(Integer::parseInt)
                .map { it * 2 }
                .require { it < 0 } // This should fail and give JobStatus.PermanentFailure
                .map { it + 4 }
                .end { it > 0 }
        }

        assertEquals(JobStatus.PermanentFailure, result)
    }

    @Test
    fun testNullaryEndIsSuccess() {
        val res = runBlocking { jobOne.end() }
        assertEquals(JobStatus.Success, res)
    }

    @Test
    fun testNullaryEndNonSuccess() {
        val res = runBlocking { jobOne.require { false }.end() }
        assertNotEquals(JobStatus.Success, res)
    }

    @Test
    fun testSideeffectCalled() {
        var effectCalled = false
        val res = runBlocking {
            jobOne.sideEffect { effectCalled = true }.end()
        }
        assertEquals(JobStatus.Success, res)
        assertTrue { effectCalled }
    }

    @Test
    fun testSideeffectNotCalled() {
        var effectCalled = false
        runBlocking {
            jobOne.require { false }.sideEffect { effectCalled = true }.end()
        }
        assertFalse { effectCalled }
    }

    private fun lolz() = 10

    @Test
    fun testInlineMapWorks() {
        runBlocking {
            jobOne.map { lolz() }
        }.let {
            assertEquals(10, it.value)
        }
    }

    @Test
    fun testRequireWithLogMessage() {
        val state = runBlocking {
            jobOne
                .require("This predicate must be true") { false }
                .end()
        }

        assertEquals(JobStatus.PermanentFailure, state)
    }

    @Test
    fun testExecuteWithLogMessage() {
        val state = runBlocking {
            jobOne
                .execute("This predicate must be true") { false }
                .end()
        }

        assertEquals(JobStatus.TransientFailure, state)
    }

    @Test
    fun testAdvanceIfWithLogMessage() {
        val state = runBlocking {
            jobOne
                .advanceIf("This predicate must be true") { false }
                .end()
        }

        assertEquals(JobStatus.Success, state)
    }

    @Test
    fun testPerformSuccess() {
        val state = runBlocking {
            jobOne
                .executeToResult { WorkerResult.Success }
                .end()
        }

        assertEquals(JobStatus.Success, state)
    }

    @Test
    fun testPerformRetry() {
        val state = runBlocking {
            jobOne
                .executeToResult { WorkerResult.Retry }
                .end()
        }

        assertEquals(JobStatus.TransientFailure, state)
    }

    @Test
    fun testPerformFailure() {
        val state = runBlocking {
            jobOne
                .executeToResult { WorkerResult.Failure }
                .end()
        }

        assertEquals(JobStatus.PermanentFailure, state)
    }

    @Test
    fun testPerformHaltPipe() {
        val state = runBlocking {
            jobOne
                .executeToResult { WorkerResult.Success }
                .executeToResult { WorkerResult.Retry }     // Execution should not continue after this
                .executeToResult { WorkerResult.Success }
                .end()
        }

        assertEquals(JobStatus.TransientFailure, state)
    }

    @Test
    fun testPerformSuccesfullPipe() {
        val state = runBlocking {
            jobOne
                .executeToResult { WorkerResult.Success }
                .executeToResult { WorkerResult.Success }
                .executeToResult { WorkerResult.Success }
                .end()
        }

        assertEquals(JobStatus.Success, state)
    }

    @Test
    fun testMixedOperations() {
        val state = runBlocking {
            jobOne
                .require { true }
                .executeToResult { WorkerResult.Success }
                .execute { true }
                .end()
        }

        assertEquals(JobStatus.Success, state)
    }

    @Test
    fun testBranchIfSuccess(){
        val state = runBlocking {
            jobOne
                .branchIf(true){
                    it
                        .execute { true }
                        .jobStatus()
                }
                .end()
        }

        assertEquals(JobStatus.Success, state)
    }

    @Test
    fun testBranchIfFailure(){
        val state = runBlocking {
            jobOne
                .branchIf(true){
                    it
                        .execute { false }
                        .jobStatus()
                }
                .end()
        }

        assertEquals(JobStatus.TransientFailure, state)
    }

    @Test
    fun testBranchIfSuccesHalt(){
        val state = runBlocking {
            jobOne
                .branchIf(true){
                    it
                        .execute { true }
                        .end()
                }
                .execute { false }      // This shouldn't be run as the branch returned with success
                .end()
        }

        assertEquals(JobStatus.Success, state)
    }

    @Test
    fun testBranchIfFailureHalt(){
        val state = runBlocking {
            jobOne
                .branchIf(true){
                    it
                        .execute { false }
                        .end()
                }
                .execute { true }      // This shouldn't be run as the branch returned with failure
                .end()
        }

        assertEquals(JobStatus.TransientFailure, state)
    }

    @Test
    fun testSeveralBranches(){
        val state = runBlocking {
            jobOne
                .branchIf(false){ // This branch is never run
                    it
                        .execute { false }
                        .end()
                }
                .branchIf(true){
                    it
                        .execute { true }
                        .end()
                }
                .execute { false }      // This shouldn't be run as the branch returned with success
                .end()
        }

        assertEquals(JobStatus.Success, state)
    }

    @Test
    fun testPredicateBranch(){
        val state = runBlocking {
            jobOne
                .map { "test" }
                .branchIf({ it == "test"} ){ // This branch is never run
                    it
                        .execute { true }
                        .end()
                }
                .end()
        }

        assertEquals(JobStatus.Success, state)
    }

    @Test
    fun testOnTransientFailureWithTransientFailure(){
        var hasRunTransientFailure = false

        val state = runBlocking {
            jobOne
                .execute { false }
                .onTransientFailure { hasRunTransientFailure = true }
                .end()
        }

        assertEquals(JobStatus.TransientFailure, state)
        assertTrue(hasRunTransientFailure)
    }

    @Test
    fun testOnTransientFailureWithPermanentFailure(){
        var hasRunTransientFailure = false

        val state = runBlocking {
            jobOne
                .require { false }
                .onTransientFailure { hasRunTransientFailure = true }
                .end()
        }

        assertEquals(JobStatus.PermanentFailure, state)
        assertFalse(hasRunTransientFailure)
    }

    @Test
    fun testOnTransientFailureWithIncomplete(){
        var hasRunTransientFailure = false

        val state = runBlocking {
            jobOne
                .require { true }
                .onTransientFailure { hasRunTransientFailure = true }
                .end()
        }

        assertEquals(JobStatus.Success, state)
        assertFalse(hasRunTransientFailure)
    }

    @Test
    fun testOnPermanentFailureWithTransientFailure(){
        var hasRunPermanentFailure = false

        val state = runBlocking {
            jobOne
                .execute { false }
                .onPermanentFailure { hasRunPermanentFailure = true }
                .end()
        }

        assertEquals(JobStatus.TransientFailure, state)
        assertFalse(hasRunPermanentFailure)
    }

    @Test
    fun testOnPermanentFailureWithPermanentFailure(){
        var hasRunPermanentFailure = false

        val state = runBlocking {
            jobOne
                .require { false }
                .onPermanentFailure { hasRunPermanentFailure = true }
                .end()
        }

        assertEquals(JobStatus.PermanentFailure, state)
        assertTrue(hasRunPermanentFailure)
    }

    @Test
    fun testOnPermanentFailureWithIncomplete(){
        var hasRunPermanentFailure = false

        val state = runBlocking {
            jobOne
                .require { true }
                .onPermanentFailure { hasRunPermanentFailure = true }
                .end()
        }

        assertEquals(JobStatus.Success, state)
        assertFalse(hasRunPermanentFailure)
    }
}