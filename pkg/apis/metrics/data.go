// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// system.metrics
	chSystemMetricQuery                                    = "query"
	chSystemMetricMerge                                    = "merge"
	chSystemMetricPartMutation                             = "part_mutation"
	chSystemMetricReplicatedFetch                          = "replicated_fetch"
	chSystemMetricReplicatedSend                           = "replicated_send"
	chSystemMetricReplicatedChecks                         = "replicated_checks"
	chSystemMetricBackgroundPoolTask                       = "background_pool_task"
	chSystemMetricBackgroundSchedulePoolTask               = "background_schedule_pool_task"
	chSystemMetricDiskSpaceReservedForMerge                = "disk_space_reserved_for_merge"
	chSystemMetricDistributedSend                          = "distributed_send"
	chSystemMetricQueryPreempted                           = "query_preempted"
	chSystemMetricTCPConnection                            = "tcp_connection"
	chSystemMetricHTTPConnection                           = "http_connection"
	chSystemMetricInterserverConnection                    = "interserver_connection"
	chSystemMetricOpenFileForRead                          = "open_file_for_read"
	chSystemMetricOpenFileForWrite                         = "open_file_for_write"
	chSystemMetricRead                                     = "read"
	chSystemMetricWrite                                    = "write"
	chSystemMetricSendExternalTables                       = "send_external_tables"
	chSystemMetricQueryThread                              = "query_thread"
	chSystemMetricReadonlyReplica                          = "readonly_replica"
	chSystemMetricLeaderReplica                            = "leader_replica"
	chSystemMetricMemoryTracking                           = "memory_tracking"
	chSystemMetricMemoryTrackingInBackgroundProcessingPool = "memory_tracking_in_background_processing_pool"
	chSystemMetricMemoryTrackingInBackgroundSchedulePool   = "memory_tracking_in_background_schedule_pool"
	chSystemMetricMemoryTrackingForMerges                  = "memory_tracking_for_merges"
	chSystemMetricLeaderElection                           = "leader_election"
	chSystemMetricEphemeralNode                            = "ephemeral_node"
	chSystemMetricZooKeeperSession                         = "zookeeper_session"
	chSystemMetricZooKeeperWatch                           = "zookeeper_watch"
	chSystemMetricZooKeeperRequest                         = "zookeeper_request"
	chSystemMetricDelayedInserts                           = "delayed_inserts"
	chSystemMetricContextLockWait                          = "context_lock_wait"
	chSystemMetricStorageBufferRows                        = "storage_buffer_rows"
	chSystemMetricStorageBufferBytes                       = "storage_buffer_bytes"
	chSystemMetricDictCacheRequests                        = "dict_cache_requests"
	chSystemMetricRevision                                 = "revision"
	chSystemMetricVersionInteger                           = "version_integer"
	chSystemMetricRWLockWaitingReaders                     = "rw_lock_waiting_readers"
	chSystemMetricRWLockWaitingWriters                     = "rw_lock_waiting_writers"
	chSystemMetricRWLockActiveReaders                      = "rw_lock_active_readers"
	chSystemMetricRWLockActiveWriters                      = "rw_lock_active_writers"
)

const (
	// system.asynchronous_metrics
	chSystemAsyncMetricJemallocBackgroundThreadRunInterval = "jemalloc_background_thread_run_interval"
	chSystemAsyncMetricJemallocBackgroundThreadNumRuns     = "jemalloc_background_thread_num_runs"
	chSystemAsyncMetricJemallocBackgroundThreadNumThreads  = "jemalloc_background_thread_num_threads"
	chSystemAsyncMetricJemallocRetained                    = "jemalloc_retained"
	chSystemAsyncMetricJemallocMapped                      = "jemalloc_mapped"
	chSystemAsyncMetricJemallocResident                    = "jemalloc_resident"
	chSystemAsyncMetricJemallocMetadataThp                 = "jemalloc_metadata_thp"
	chSystemAsyncMetricJemallocMetadata                    = "jemalloc_metadata"
	chSystemAsyncMetricUncompressedCacheCells              = "uncompressed_cache_cells"
	chSystemAsyncMetricMarkCacheFiles                      = "mark_cache_files"
	chSystemAsyncMetricMarkCacheBytes                      = "mark_cache_bytes"
	chSystemAsyncMetricJemallocAllocated                   = "jemalloc_allocated"
	chSystemAsyncMetricReplicasMaxQueueSize                = "replicas_max_queue_size"
	chSystemAsyncMetricUncompressedCacheBytes              = "uncompressed_cache_bytes"
	chSystemAsyncMetricUptime                              = "uptime"
	chSystemAsyncMetricCompiledExpressionCacheCount        = "compiled_expression_cache_count"
	chSystemAsyncMetricReplicasMaxInsertsInQueue           = "replicas_max_inserts_in_queue"
	chSystemAsyncMetricReplicasMaxMergesInQueue            = "replicas_max_merges_in_queue"
	chSystemAsyncMetricReplicasMaxRelativeDelay            = "replicas_max_relative_delay"
	chSystemAsyncMetricJemallocActive                      = "jemalloc_active"
	chSystemAsyncMetricReplicasSumQueueSize                = "replicas_sum_queue_size"
	chSystemAsyncMetricReplicasSumInsertsInQueue           = "replicas_sum_inserts_in_queue"
	chSystemAsyncMetricReplicasMaxAbsoluteDelay            = "replicas_max_absolute_delay"
	chSystemAsyncMetricReplicasSumMergesInQueue            = "replicas_sum_merges_in_queue"
	chSystemAsyncMetricMaxPartCountForPartition            = "max_part_count_for_partition"
)

var (
	// clickhouseMetricsDescriptions keeps description of mertics.
	// Those descriptions are copy+pasted from system.metrics and system.asynchronous_metrics
	// SELECT description
	// FROM system.metrics
	// LIMIT 5
	//
	// ┌─description────────────────────────────────┐
	// │ Number of executing queries                │
	// │ Number of executing background merges      │
	// │ Number of mutations (ALTER DELETE/UPDATE)  │
	// │ Number of data parts fetching from replica │
	// │ Number of data parts sending to replicas   │
	// └────────────────────────────────────────────┘
	clickhouseMetricsDescriptions = map[string]*prometheus.Desc{

		// system.metrics
		chSystemMetricQuery: newDescription(chSystemMetricQuery,
			"Number of executing queries"),
		chSystemMetricMerge: newDescription(chSystemMetricMerge,
			"Number of executing background merges"),
		chSystemMetricPartMutation: newDescription(chSystemMetricPartMutation,
			"Number of mutations (ALTER DELETE/UPDATE)"),
		chSystemMetricReplicatedFetch: newDescription(chSystemMetricReplicatedFetch,
			"Number of data parts fetching from replica"),
		chSystemMetricReplicatedSend: newDescription(chSystemMetricReplicatedSend,
			"Number of data parts sending to replicas"),
		chSystemMetricReplicatedChecks: newDescription(chSystemMetricReplicatedChecks,
			"Number of data parts checking for consistency"),
		chSystemMetricBackgroundPoolTask: newDescription(chSystemMetricBackgroundPoolTask,
			"Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches or replication queue bookkeeping)"),
		chSystemMetricBackgroundSchedulePoolTask: newDescription(chSystemMetricBackgroundSchedulePoolTask,
			"Number of active tasks in BackgroundSchedulePool. This pool is used for periodic tasks of ReplicatedMergeTree like cleaning old data parts, altering data parts, replica re-initialization, etc."),
		chSystemMetricDiskSpaceReservedForMerge: newDescription(chSystemMetricDiskSpaceReservedForMerge,
			"Disk space reserved for currently running background merges. It is slightly more than total size of currently merging parts."),
		chSystemMetricDistributedSend: newDescription(chSystemMetricDistributedSend,
			"Number of connections sending data, that was INSERTed to Distributed tables, to remote servers. Both synchronous and asynchronous mode."),
		chSystemMetricQueryPreempted: newDescription(chSystemMetricQueryPreempted,
			"Number of queries that are stopped and waiting due to 'priority' setting."),
		chSystemMetricTCPConnection: newDescription(chSystemMetricTCPConnection,
			"Number of connections to TCP server (clients with native interface)"),
		chSystemMetricHTTPConnection: newDescription(chSystemMetricHTTPConnection,
			"Number of connections to HTTP server"),
		chSystemMetricInterserverConnection: newDescription(chSystemMetricInterserverConnection,
			"Number of connections from other replicas to fetch parts"),
		chSystemMetricOpenFileForRead: newDescription(chSystemMetricOpenFileForRead,
			"Number of files open for reading"),
		chSystemMetricOpenFileForWrite: newDescription(chSystemMetricOpenFileForWrite,
			"Number of files open for writing"),
		chSystemMetricRead: newDescription(chSystemMetricRead,
			"Number of read (read, pread, io_getevents, etc.) syscalls in fly"),
		chSystemMetricWrite: newDescription(chSystemMetricWrite,
			"Number of write (write, pwrite, io_getevents, etc.) syscalls in fly"),
		chSystemMetricSendExternalTables: newDescription(chSystemMetricSendExternalTables,
			"Number of connections that are sending data for external tables to remote servers. External tables are used to implement GLOBAL IN and GLOBAL JOIN operators with distributed subqueries."),
		chSystemMetricQueryThread: newDescription(chSystemMetricQueryThread,
			"Number of query processing threads"),
		chSystemMetricReadonlyReplica: newDescription(chSystemMetricReadonlyReplica,
			"Number of Replicated tables that are currently in readonly state due to re-initialization after ZooKeeper session loss or due to startup without ZooKeeper configured."),
		chSystemMetricLeaderReplica: newDescription(chSystemMetricLeaderReplica,
			"Number of Replicated tables that are leaders. Leader replica is responsible for assigning merges, cleaning old blocks for deduplications and a few more bookkeeping tasks. There may be no more than one leader across all replicas at one moment of time. If there is no leader it will be elected soon or it indicate an issue."),
		chSystemMetricMemoryTracking: newDescription(chSystemMetricMemoryTracking,
			"Total amount of memory (bytes) allocated in currently executing queries. Note that some memory allocations may not be accounted."),
		chSystemMetricMemoryTrackingInBackgroundProcessingPool: newDescription(chSystemMetricMemoryTrackingInBackgroundProcessingPool,
			"Total amount of memory (bytes) allocated in background processing pool (that is dedicated for backround merges, mutations and fetches). Note that this value may include a drift when the memory was allocated in a context of background processing pool and freed in other context or vice-versa. This happens naturally due to caches for tables indexes and doesn't indicate memory leaks."),
		chSystemMetricMemoryTrackingInBackgroundSchedulePool: newDescription(chSystemMetricMemoryTrackingInBackgroundSchedulePool,
			"Total amount of memory (bytes) allocated in background schedule pool (that is dedicated for bookkeeping tasks of Replicated tables)."),
		chSystemMetricMemoryTrackingForMerges: newDescription(chSystemMetricMemoryTrackingForMerges,
			"Total amount of memory (bytes) allocated for background merges. Included in MemoryTrackingInBackgroundProcessingPool. Note that this value may include a drift when the memory was allocated in a context of background processing pool and freed in other context or vice-versa. This happens naturally due to caches for tables indexes and doesn't indicate memory leaks."),
		chSystemMetricLeaderElection: newDescription(chSystemMetricLeaderElection,
			"Number of Replicas participating in leader election. Equals to total number of replicas in usual cases."),
		chSystemMetricEphemeralNode: newDescription(chSystemMetricEphemeralNode,
			"Number of ephemeral nodes hold in ZooKeeper."),
		chSystemMetricZooKeeperSession: newDescription(chSystemMetricZooKeeperSession,
			"Number of sessions (connections) to ZooKeeper. Should be no more than one, because using more than one connection to ZooKeeper may lead to bugs due to lack of linearizability (stale reads) that ZooKeeper consistency model allows."),
		chSystemMetricZooKeeperWatch: newDescription(chSystemMetricZooKeeperWatch,
			"Number of watches (event subscriptions) in ZooKeeper."),
		chSystemMetricZooKeeperRequest: newDescription(chSystemMetricZooKeeperRequest,
			"Number of requests to ZooKeeper in fly."),
		chSystemMetricDelayedInserts: newDescription(chSystemMetricDelayedInserts,
			"Number of INSERT queries that are throttled due to high number of active data parts for partition in a MergeTree table."),
		chSystemMetricContextLockWait: newDescription(chSystemMetricContextLockWait,
			"Number of threads waiting for lock in Context. This is global lock."),
		chSystemMetricStorageBufferRows: newDescription(chSystemMetricStorageBufferRows,
			"Number of rows in buffers of Buffer tables"),
		chSystemMetricStorageBufferBytes: newDescription(chSystemMetricStorageBufferBytes,
			"Number of bytes in buffers of Buffer tables"),
		chSystemMetricDictCacheRequests: newDescription(chSystemMetricDictCacheRequests,
			"Number of requests in fly to data sources of dictionaries of cache type."),
		chSystemMetricRevision: newDescription(chSystemMetricRevision,
			"Revision of the server. It is a number incremented for every release or release candidate except patch releases."),
		chSystemMetricVersionInteger: newDescription(chSystemMetricVersionInteger,
			"Version of the server in a single integer number in base-1000. For example, version 11.22.33 is translated to 11022033."),
		chSystemMetricRWLockWaitingReaders: newDescription(chSystemMetricRWLockWaitingReaders,
			"Number of threads waiting for read on a table RWLock."),
		chSystemMetricRWLockWaitingWriters: newDescription(chSystemMetricRWLockWaitingWriters,
			"Number of threads waiting for write on a table RWLock."),
		chSystemMetricRWLockActiveReaders: newDescription(chSystemMetricRWLockActiveReaders,
			"Number of threads holding read lock in a table RWLock."),
		chSystemMetricRWLockActiveWriters: newDescription(chSystemMetricRWLockActiveWriters,
			"Number of threads holding write lock in a table RWLock."),

		// system.asynchronous_metrics
		chSystemAsyncMetricJemallocBackgroundThreadRunInterval: newDescription(chSystemAsyncMetricJemallocBackgroundThreadRunInterval,
			"jemalloc: background_thread.run_interval"),
		chSystemAsyncMetricJemallocBackgroundThreadNumRuns: newDescription(chSystemAsyncMetricJemallocBackgroundThreadNumRuns,
			"jemalloc: background_thread.num_runs"),
		chSystemAsyncMetricJemallocBackgroundThreadNumThreads: newDescription(chSystemAsyncMetricJemallocBackgroundThreadNumThreads,
			"jemalloc: background_thread.num_threads"),
		chSystemAsyncMetricJemallocRetained: newDescription(chSystemAsyncMetricJemallocRetained,
			"jemalloc: retained"),
		chSystemAsyncMetricJemallocMapped: newDescription(chSystemAsyncMetricJemallocMapped,
			"jemalloc: mapped"),
		chSystemAsyncMetricJemallocResident: newDescription(chSystemAsyncMetricJemallocResident,
			"jemalloc: resident"),
		chSystemAsyncMetricJemallocMetadataThp: newDescription(chSystemAsyncMetricJemallocMetadataThp,
			"jemalloc: metadata_thp"),
		chSystemAsyncMetricJemallocMetadata: newDescription(chSystemAsyncMetricJemallocMetadata,
			"jemalloc: metadata"),
		chSystemAsyncMetricUncompressedCacheCells: newDescription(chSystemAsyncMetricUncompressedCacheCells,
			"Uncompressed cache cells"),
		chSystemAsyncMetricMarkCacheFiles: newDescription(chSystemAsyncMetricMarkCacheFiles,
			"Mark cache files"),
		chSystemAsyncMetricMarkCacheBytes: newDescription(chSystemAsyncMetricMarkCacheBytes,
			"Mark cache bytes"),
		chSystemAsyncMetricJemallocAllocated: newDescription(chSystemAsyncMetricJemallocAllocated,
			"jemalloc: allocated"),
		chSystemAsyncMetricReplicasMaxQueueSize: newDescription(chSystemAsyncMetricReplicasMaxQueueSize,
			"Replicas max queue size"),
		chSystemAsyncMetricUncompressedCacheBytes: newDescription(chSystemAsyncMetricUncompressedCacheBytes,
			"Uncompressed cache bytes"),
		chSystemAsyncMetricUptime: newDescription(chSystemAsyncMetricUptime,
			"Uptime"),
		chSystemAsyncMetricCompiledExpressionCacheCount: newDescription(chSystemAsyncMetricCompiledExpressionCacheCount,
			"Compiled expression cache count"),
		chSystemAsyncMetricReplicasMaxInsertsInQueue: newDescription(chSystemAsyncMetricReplicasMaxInsertsInQueue,
			"Replicas max inserts in queue"),
		chSystemAsyncMetricReplicasMaxMergesInQueue: newDescription(chSystemAsyncMetricReplicasMaxMergesInQueue,
			"Replicas max merges in queue"),
		chSystemAsyncMetricReplicasMaxRelativeDelay: newDescription(chSystemAsyncMetricReplicasMaxRelativeDelay,
			"Replicas max relative delay"),
		chSystemAsyncMetricJemallocActive: newDescription(chSystemAsyncMetricJemallocActive,
			"jemalloc: active"),
		chSystemAsyncMetricReplicasSumQueueSize: newDescription(chSystemAsyncMetricReplicasSumQueueSize,
			"Replicas sum queue size"),
		chSystemAsyncMetricReplicasSumInsertsInQueue: newDescription(chSystemAsyncMetricReplicasSumInsertsInQueue,
			"Replicas sum inserts in queue"),
		chSystemAsyncMetricReplicasMaxAbsoluteDelay: newDescription(chSystemAsyncMetricReplicasMaxAbsoluteDelay,
			"Replicas max absolute delay"),
		chSystemAsyncMetricReplicasSumMergesInQueue: newDescription(chSystemAsyncMetricReplicasSumMergesInQueue,
			"Replicas sum merges in queue"),
		chSystemAsyncMetricMaxPartCountForPartition: newDescription(chSystemAsyncMetricMaxPartCountForPartition,
			"Max part count for partition"),
	}

	// metricsNames keeps exact (case-sensitive) metric names copy+pasted
	// from system.metrics and system.asynchronous_metrics tables.
	//
	// SELECT metric
	// FROM system.metrics
	// LIMIT 5
	//
	// ┌─metric──────────┐
	// │ Query           │
	// │ Merge           │
	// │ PartMutation    │
	// │ ReplicatedFetch │
	// │ ReplicatedSend  │
	// └─────────────────┘
	metricsNames = map[string]string{

		// system.metrics
		chSystemMetricQuery:                                    "Query",
		chSystemMetricMerge:                                    "Merge",
		chSystemMetricPartMutation:                             "PartMutation",
		chSystemMetricReplicatedFetch:                          "ReplicatedFetch",
		chSystemMetricReplicatedSend:                           "ReplicatedSend",
		chSystemMetricReplicatedChecks:                         "ReplicatedChecks",
		chSystemMetricBackgroundPoolTask:                       "BackgroundPoolTask",
		chSystemMetricBackgroundSchedulePoolTask:               "BackgroundSchedulePoolTask",
		chSystemMetricDiskSpaceReservedForMerge:                "DiskSpaceReservedForMerge",
		chSystemMetricDistributedSend:                          "DistributedSend",
		chSystemMetricQueryPreempted:                           "QueryPreempted",
		chSystemMetricTCPConnection:                            "TCPConnection",
		chSystemMetricHTTPConnection:                           "HTTPConnection",
		chSystemMetricInterserverConnection:                    "InterserverConnection",
		chSystemMetricOpenFileForRead:                          "OpenFileForRead",
		chSystemMetricOpenFileForWrite:                         "OpenFileForWrite",
		chSystemMetricRead:                                     "Read",
		chSystemMetricWrite:                                    "Write",
		chSystemMetricSendExternalTables:                       "SendExternalTables",
		chSystemMetricQueryThread:                              "QueryThread",
		chSystemMetricReadonlyReplica:                          "ReadonlyReplica",
		chSystemMetricLeaderReplica:                            "LeaderReplica",
		chSystemMetricMemoryTracking:                           "MemoryTracking",
		chSystemMetricMemoryTrackingInBackgroundProcessingPool: "MemoryTrackingInBackgroundProcessingPool",
		chSystemMetricMemoryTrackingInBackgroundSchedulePool:   "MemoryTrackingInBackgroundSchedulePool",
		chSystemMetricMemoryTrackingForMerges:                  "MemoryTrackingForMerges",
		chSystemMetricLeaderElection:                           "LeaderElection",
		chSystemMetricEphemeralNode:                            "EphemeralNode",
		chSystemMetricZooKeeperSession:                         "ZooKeeperSession",
		chSystemMetricZooKeeperWatch:                           "ZooKeeperWatch",
		chSystemMetricZooKeeperRequest:                         "ZooKeeperRequest",
		chSystemMetricDelayedInserts:                           "DelayedInserts",
		chSystemMetricContextLockWait:                          "ContextLockWait",
		chSystemMetricStorageBufferRows:                        "StorageBufferRows",
		chSystemMetricStorageBufferBytes:                       "StorageBufferBytes",
		chSystemMetricDictCacheRequests:                        "DictCacheRequests",
		chSystemMetricRevision:                                 "Revision",
		chSystemMetricVersionInteger:                           "VersionInteger",
		chSystemMetricRWLockWaitingReaders:                     "RWLockWaitingReaders",
		chSystemMetricRWLockWaitingWriters:                     "RWLockWaitingWriters",
		chSystemMetricRWLockActiveReaders:                      "RWLockActiveReaders",
		chSystemMetricRWLockActiveWriters:                      "RWLockActiveWriters",

		// system.asynchronous_metrics
		chSystemAsyncMetricJemallocBackgroundThreadRunInterval: "jemalloc.background_thread.run_interval",
		chSystemAsyncMetricJemallocBackgroundThreadNumRuns:     "jemalloc.background_thread.num_runs",
		chSystemAsyncMetricJemallocBackgroundThreadNumThreads:  "jemalloc.background_thread.num_threads",
		chSystemAsyncMetricJemallocRetained:                    "jemalloc.retained",
		chSystemAsyncMetricJemallocMapped:                      "jemalloc.mapped",
		chSystemAsyncMetricJemallocResident:                    "jemalloc.resident",
		chSystemAsyncMetricJemallocMetadataThp:                 "jemalloc.metadata_thp",
		chSystemAsyncMetricJemallocMetadata:                    "jemalloc.metadata",
		chSystemAsyncMetricUncompressedCacheCells:              "UncompressedCacheCells",
		chSystemAsyncMetricMarkCacheFiles:                      "MarkCacheFiles",
		chSystemAsyncMetricMarkCacheBytes:                      "MarkCacheBytes",
		chSystemAsyncMetricJemallocAllocated:                   "jemalloc.allocated",
		chSystemAsyncMetricReplicasMaxQueueSize:                "ReplicasMaxQueueSize",
		chSystemAsyncMetricUncompressedCacheBytes:              "UncompressedCacheBytes",
		chSystemAsyncMetricUptime:                              "Uptime",
		chSystemAsyncMetricCompiledExpressionCacheCount:        "CompiledExpressionCacheCount",
		chSystemAsyncMetricReplicasMaxInsertsInQueue:           "ReplicasMaxInsertsInQueue",
		chSystemAsyncMetricReplicasMaxMergesInQueue:            "ReplicasMaxMergesInQueue",
		chSystemAsyncMetricReplicasMaxRelativeDelay:            "ReplicasMaxRelativeDelay",
		chSystemAsyncMetricJemallocActive:                      "jemalloc.active",
		chSystemAsyncMetricReplicasSumQueueSize:                "ReplicasSumQueueSize",
		chSystemAsyncMetricReplicasSumInsertsInQueue:           "ReplicasSumInsertsInQueue",
		chSystemAsyncMetricReplicasMaxAbsoluteDelay:            "ReplicasMaxAbsoluteDelay",
		chSystemAsyncMetricReplicasSumMergesInQueue:            "ReplicasSumMergesInQueue",
		chSystemAsyncMetricMaxPartCountForPartition:            "MaxPartCountForPartition",
	}
)
