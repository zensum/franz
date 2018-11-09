package franz.engine.kafka_one

import franz.JobStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.lang.IllegalStateException

private fun <K, V> Map<K, V>.getOrFail(k: K): V = get(k)
    ?: throw IllegalStateException("Got null when trying to access value for key $k in map ${this.keys}")

private fun findCommittableOffsets(x: Map<JobId, JobStatus>) = x
        .toList()
        .groupBy { it.first.first }
        .map { (_, values) ->
            values.sortedBy { (key, _) -> key.second }
                    .takeWhile { (_, status) -> status.isDone() }
                    .lastOrNull()?.first
        }
        .filterNotNull()
        .toMap()
        .mapValues { (_, v) -> OffsetAndMetadata(v + 1) }

data class JobStatuses<T, U>(
        private val jobStatuses: Map<JobId, JobStatus> = emptyMap<JobId, JobStatus>(),
        private val records: Map<JobId, ConsumerRecord<T, U>> = emptyMap()
) {
    fun update(updates: Map<JobId, JobStatus>) = copy(jobStatuses = jobStatuses + updates)
    fun committableOffsets() = findCommittableOffsets(jobStatuses)
    fun removeCommitted(committed: Map<TopicPartition, OffsetAndMetadata>) = if (committed.isEmpty()) this else
        copy(
                jobStatuses = jobStatuses.filterKeys { (topicPartition, offset) ->
                    val committedOffset = committed[topicPartition]?.offset() ?: -1
                    offset >= committedOffset
                },
                records = records.filterValues {
                    val committedOffset = committed[it.topicPartition()]?.offset() ?: -1
                    it.offset() >= committedOffset
                }
        )
    fun stateCounts() = jobStatuses.values.map { it::class.java.name!! }.groupBy { it }.mapValues { it.value.count() }
    private fun changeBatch(jobs: Iterable<JobId>, status: JobStatus)
            = update(jobs.map { it to status }.toMap())
    fun addJobs(jobs: Iterable<ConsumerRecord<T, U>>) =
            changeBatch(jobs.map { it.jobId() }, JobStatus.Incomplete)
                    .copy(records = records + jobs.map { it.jobId() to it })
    fun rescheduleTransientFailures(): Pair<JobStatuses<T, U>, List<ConsumerRecord<T, U>>> =
        jobStatuses
            .filterValues { it.mayRetry() }
            .keys.let { jobIds: Set<JobId> ->
            changeBatch(jobIds, JobStatus.Retry) to jobIds.map(records::getOrFail)
        }
}
