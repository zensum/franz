package franz

// A worker processing processing a jobstate to a jobstatus
interface Worker {
    suspend fun process(
        message: JobState<Message<String, ByteArray>>
    ): JobStatus
}