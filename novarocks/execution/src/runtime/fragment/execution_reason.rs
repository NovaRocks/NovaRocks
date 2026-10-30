// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The Execution namespace's stable reason allocation. Rust rejects duplicate
//! discriminants. Producers retain classification and optional fact ownership;
//! this vocabulary contains no cross-domain classifier or diagnostic parsing.

use novarocks_failure_contract::{FailureReason, domain};
use std::num::NonZeroU16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub(crate) enum ExecutionReason {
    ScalarMissingQueryMemory = 1,
    PreparationMissingQueryMemory = 2,
    PreparationStopped = 3,
    WriterMissingOutcome = 10,
    WriterPanicked = 11,
    WriterAbortTimeout = 12,
    WriterAcceptedRowsOverflow = 13,
    WriterCommitFragmentLimit = 14,
    WriterInterrupted = 15,
    SourceInputEdge = 16,
    SourceInputEos = 17,
    SourceRequestedInput = 18,
    SourceMissingRuntimeBinding = 19,
    SourceColumnCount = 20,
    ReceiverUnexpectedRootGrant = 21,
    FilterArrow = 24,
    SetInvalidStage = 25,
    SetDuplicateCompletion = 26,
    ReceiverInputAfterFinish = 27,
    JoinGroupUnavailable = 28,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    JoinBuildRowUnavailable = 29,
    JoinPresenceOnly = 30,
    JoinRowIdExtent = 31,
    LocalExchangeMissingBinding = 32,
    AsyncSinkInvalidCapacity = 40,
    AsyncSinkNotActivated = 41,
    AsyncSinkReceiverClosed = 42,
    AsyncSinkMissingPermit = 43,
    AsyncSinkInputAfterFinish = 44,
    AsyncSinkInterrupted = 46,
    AsyncSinkPanicked = 47,
    StatisticsInputAfterFinish = 48,
    StatisticsMissingRootGrant = 49,
    ReceiverMissingRootGrant = 50,
    ReceiverForeignRootTask = 51,
    RootInvalidDop = 52,
    RootInputAfterFinish = 53,
    RootFinishUnderflow = 54,
    RootNotActivated = 55,
    #[cfg(test)]
    TestWriteFailure = 60,
    FragmentIoUnavailable = 70,
    FragmentIoTimeout = 71,
    FragmentIoRemoteRejected = 72,
    FragmentIoInvalidResponse = 73,
    FragmentIoCancelled = 74,
    FragmentIoInternal = 75,
    FanoutNotPrepared = 91,
    FanoutInputClosed = 92,
    FanoutBranchOutput = 93,
    FanoutInvalidRoute = 94,
    FanoutCleanupPanicked = 95,
    FanoutEmptyBranches = 96,
    ChildInputSlotOccupied = 121,
    ChildUnexpectedInputRequest = 122,
    ChildInvalidExitWatch = 123,
    ChildCleanupPanicked = 124,
    WriterPartialDemandClosed = 125,
    TableFinishFinalInputClosed = 126,
    TableFinishEarlyFinalOutput = 127,
    DriverPanicked = 128,
    DriverExitHookPanicked = 129,
    DriverDuplicateRootGrant = 130,
    DriverMissingOperator = 131,
    DriverTerminalOutput = 132,
    SourcePageChannels = 133,
    SourcePagePositions = 134,
    SourcePageBinding = 135,
    ExecutionRuntimeMissing = 136,
    DriverExecutorShutdown = 137,
    DriverUnrequestedCancellation = 138,
    DriverDeadlineExceeded = 139,
    DriverMissingObservable = 140,
    DriverDependencyScheduleRefused = 141,
    WriterAlreadyBound = 142,
    PipelineMultipleEndpoint = 143,
    PipelineMissingEndpoint = 144,
    PipelineEndpointPosition = 145,
    RootDopRebound = 146,
    RootMetadataExtent = 147,
    BindingMissingAssignment = 152,
    BindingExtraAssignment = 153,
    BindingWrongAssignmentKind = 154,
    BindingInvalidAssignment = 155,
    BindingSchemaMismatch = 156,
    BindingLayoutMismatch = 157,
    BindingExpressionMismatch = 158,
    BindingRuntimeFilterMismatch = 159,
    BindingContractVersionMismatch = 160,
    PlanNodeInvariant = 161,
    PlanExpressionInvariant = 162,
    PlanLayoutInvariant = 163,
    PlanSchemaInvariant = 164,
    PlanSinkInvariant = 165,
    PlanInternalInvariant = 166,
    CacheEvictionProbability = 167,
    CachePriority = 168,
    CacheTtl = 169,
    QueryMemoryOwnerAttempt = 170,
    QueryMemoryTaskAttempt = 171,
    QueryMemoryMissingTask = 172,
    QueryMemoryQueryIdentity = 173,
    QueryMemoryMissingRuntime = 174,
    ValuesEmptyPort = 175,
    ValuesColumnAssembly = 176,
    NestedLoopBuildNotReady = 177,
    NestedLoopBuildNotLoaded = 178,
    NestedLoopMissingProbe = 179,
    NestedLoopMissingPredicate = 180,
    NestedLoopUnsupportedJoin = 181,
    NestedLoopRowIdExtent = 182,
    ChunkMissingSlot = 183,
    ChunkInvalidColumnIndex = 184,
    OperatorMemoryTrackerMissing = 185,
    SeriesDuplicateBounds = 193,
    SeriesMissingBatchRows = 194,
    SeriesInvalidBatchRows = 195,
    SeriesInputPort = 196,
    SeriesArrow = 197,
    SeriesAlreadyFailed = 198,
    TableCursorCapacity = 199,
    TableCursorErrorOrdinal = 200,
    TableCursorRowAfterCompletion = 201,
    TableCursorDuplicateCompletion = 202,
    TableCursorIncompleteParents = 203,
    TableCursorArrow = 204,
    TableCursorOutputExtent = 205,
    TableCursorInputPort = 206,
    TableCursorAlreadyFailed = 207,
    TableCursorMissingBatchRows = 208,
    TableCursorInvalidBatchRows = 209,
    ChunkSplitExceedsCharge = 210,
    ChunkSplitExceedsReservation = 211,
    WriterInputBatchRows = 212,
    WriterInputBatchBytes = 213,
    WriterQueueCapacity = 214,
    WriterAdditionalBytes = 215,
    WriterAfterTerminal = 216,
    WriterMailboxFull = 217,
    WriterMailboxClosed = 218,
    WindowNegativeOffset = 219,
    WindowRangeOffset = 220,
    WindowArrow = 221,
    ChangeEventPredicateType = 222,
    ChangeEventAssignmentType = 223,
    ChangeEventAlreadyFailed = 224,
    FilterMissingSession = 225,
    FilterUnauthorizedBinding = 226,
    FilterContractMismatch = 227,
    FilterRoleMismatch = 228,
    FilterInvalidPartitionCount = 229,
    FilterResourceLimit = 230,
    FilterSessionClosed = 231,
    FilterArrowApplication = 232,
    SchemaFieldBinding = 233,
    SchemaSlotRename = 234,
    SchemaLegacySlot = 235,
    SchemaDuplicateSlot = 236,
    SchemaCanonicalLayout = 237,
    SchemaIncompleteOccurrence = 238,
    SchemaMissingDerivedBinding = 239,
    SchemaDictionaryEncoding = 240,
    SchemaRunEndUnsupported = 241,
    SchemaViewUnsupported = 242,
    SchemaArrayRequest = 243,
    SchemaMetadataContract = 244,
    SchemaMutationContract = 245,
    SchemaLayoutContract = 246,
    SchemaProjectionContract = 247,
    SchemaReconciliationContract = 248,
    SchemaAssemblyContract = 249,
    SchemaTypeMismatch = 250,
    ChunkTypedChannels = 251,
    ChunkColumnRows = 252,
    ChunkColumnLayout = 253,
    ChunkColumnBinding = 254,
    // 255 retired: required values are authored by the shared value domain.
    ChunkBatchOccurrence = 256,
    ChunkSlice = 257,
    ChunkBatchLabels = 258,
    ChunkAccountingOwner = 259,
    ChunkOutputMetadata = 260,
    ChunkRecordBatch = 261,
    ChunkRecordBatchMemory = 262,
    ChunkHydrationBinding = 263,
    ChunkHydrationMapEntries = 264,
    ChunkHydrationMapArity = 265,
    // 266..=267 retired with the legacy hydration helpers.
    #[cfg(test)]
    RepeatSchemaChannels = 268,
    #[cfg(test)]
    RepeatSchemaMissingBinding = 269,
    #[cfg(test)]
    RepeatSchemaNullDeclaration = 270,
    SortKeyMissingDeclaration = 271,
    SortKeyMissingChild = 272,
    SortKeyMapChild = 273,
    SortKeyMapDeclaration = 274,
    SortKeyMapWrapper = 275,
    SortKeyPhysicalWrapper = 276,
    SortKeyLargeIntWidth = 277,
    SortKeyArrow = 278,
    SortKeyArrowMemory = 279,
    FinishRelation = 280,
    FinishCoverage = 281,
    FinishFinalGroups = 282,
    FinishRootArtifact = 283,
    FinishBinding = 284,
    FinishArrow = 285,
    FinishArrowMemory = 286,
    FinishArrowOffset = 287,
    FinishArrowUnexpected = 288,
    FinishOutputBound = 289,
    WriteRelationLayout = 290,
    WriteRelationBinding = 291,
    WriteRelationNotPrepared = 292,
    WriteRelationColumn = 293,
    WriterFrozenContract = 294,
    WriterRowCountDrift = 295,
    WriterOutputBytes = 296,
    WriterArrow = 297,
    WriterArrowMemory = 298,
    WriterArrowOffset = 299,
    WriterArrowUnexpected = 300,
    UnpivotIpcBytes = 301,
    UnpivotIpcRows = 302,
    UnpivotIpcMemory = 303,
    UnpivotIpcOffset = 304,
    UnpivotIpcUnexpected = 305,
    UnpivotIpcShape = 306,
    UnpivotContract = 307,
    UnpivotLimit = 308,
    UnpivotLiteralOffset = 309,
    #[cfg(test)]
    RepeatConfiguration = 310,
    #[cfg(test)]
    RepeatMissingSlot = 311,
    #[cfg(test)]
    RepeatMissingGrouping = 312,
    #[cfg(test)]
    RepeatOutputNotPrepared = 313,
    AggregateGroupContract = 314,
    AssertRowsContract = 315,
    AssertRowsMismatch = 316,
    AssertRowsDuplicateKey = 317,
    AssertRowsCounter = 318,
    // Retired production formatting paths; keep their IDs in independent
    // diagnostic regression fixtures without reassigning either number.
    #[cfg(test)]
    AssertRowsDisplayMemory = 319,
    #[cfg(test)]
    AssertRowsDisplay = 320,
    FinalIdentityInput = 321,
    HashJoinExecutionContract = 322,
    JoinBuildCountExtent = 323,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    JoinBuildKeyRows = 324,
    FilterContributionLength = 325,
    FilterContributionResource = 326,
    FilterContributionCodec = 327,
    FilterMembershipType = 328,
    FilterMembershipUnsupported = 329,
    FilterMembershipArray = 330,
    FilterProducerSequence = 331,
    TopNKeyArity = 332,
    TopNNonOrdered = 333,
    TopNKeyType = 334,
    TopNGroupSequence = 335,
    TopNCandidateContract = 336,
    TopNMissingKey = 337,
    TopNRowBounds = 338,
    TopNArrayType = 339,
    TopNCandidateValue = 340,
    TopNBoundLoosened = 341,
    TopNAfterFinish = 342,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    AggregateViewWrapper = 345,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    AggregateViewType = 346,
    RootStorageCarrier = 351,
    RootStorageOwner = 352,
    RootStorageByteBound = 353,
    RootStorageWorkBound = 354,
    ExchangeQueueOwnerStopped = 355,
    ExchangeObservationPoisoned = 356,
    ExchangeObservationRebound = 357,
    ExchangeStreamContract = 358,
    ExchangePartitionAllocation = 359,
    ExchangePartitionOffset = 360,
    ExchangePartitionUnexpected = 361,
    ExchangePartitionArrow = 362,
    ExchangeDictionaryKey = 363,
    ExchangePartitionRowExtent = 364,
    GroupKeyEncodingContract = 365,
    GroupKeyComponentExtent = 366,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    GroupKeyDictionaryAddress = 367,
    AggregateRequestExtent = 368,
    GroupKeyDecodeContract = 369,
    GroupKeyDecodeAllocation = 370,
    GroupKeyDecodeOffset = 371,
    GroupKeyDecodeUnexpected = 372,
    GroupKeyDecodeArrow = 373,
    GroupKeyDecodeUtf8 = 374,
    GroupKeyOutputExtent = 375,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    CompressedKeyContract = 376,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    AggregateBlockLayout = 377,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    AggregateBlockAlignment = 378,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    KeyColumnContract = 380,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    KeyColumnNarrowing = 381,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    KeyTableContract = 382,
    FilterOrderedTupleArity = 394,
    FilterOrderedTupleType = 395,
    RootStorageSchemaType = 396,
    RootStorageDowncast = 397,
    RootStorageOuterOwner = 398,
    OrderedFilterAllocation = 399,
    OrderedFilterOffset = 400,
    OrderedFilterUnexpected = 401,
    OrderedFilterArrow = 402,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    JoinHashTableContract = 411,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    JoinRowIndexCapacity = 412,
    JoinMapContract = 413,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    JoinMapExtent = 414,
    JoinSelectionContract = 415,
    JoinGatherContract = 416,
    JoinGatherAllocation = 417,
    JoinGatherOffset = 418,
    JoinGatherUnexpected = 419,
    JoinGatherArrow = 420,
    JoinArtifactContract = 422,
    #[cfg(test)]
    JoinMatchContract = 423,
    #[cfg(test)]
    JoinFinalizeContract = 424,
    JoinStoreContract = 425,
    JoinStoreRows = 426,
    JoinBuildStateContract = 428,
    MissingCompiledSchemaSite = 459,
    NlJoinContract = 460,
    DistinctSetZeroColumns = 465,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    DistinctSetMissingRow = 466,
    DistinctSetGroupSequence = 467,
    DistinctSetGroupBounds = 468,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    DistinctSetGatherAllocation = 469,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    DistinctSetGatherOffsets = 470,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    DistinctSetGatherIo = 471,
    #[expect(
        dead_code,
        reason = "Stable Execution failure reason slot retained after its producer retired; never renumber or reuse."
    )]
    DistinctSetGatherContract = 472,
    SinkMaterializationContract = 490,
    SinkRootIdentityExtent = 491,
    SinkKeyIdentityExtent = 492,
    SortOperationArrow = 493,
    SortOperationAllocation = 494,
    #[cfg(test)]
    SortRowIndexExtent = 496,
    CompiledWriterIndexExtent = 497,
    WriterRequiredValue = 498,
    WindowKernelOutputContract = 499,
    WindowArrowAllocation = 500,
    VariantKernelValue = 502,
    VariantKernelContract = 503,
    VariantKernelAllocation = 504,
    AnalyticContract = 505,
    AnalyticKeyExtent = 507,
    AnalyticArrowContract = 508,
    AnalyticArrowAllocation = 509,
    AnalyticRowIndexExtent = 511,
    ExecutorDrainDeadline = 513,
    StatisticsContract = 536,
    StatisticsResultLimit = 537,
    StatisticsArtifactExtent = 538,
    StatisticsArrowContract = 540,
    StatisticsArrowAllocation = 541,
    VariantMaterializationContract = 544,
    FilterOperationContract = 546,
    FilterArrowContract = 547,
    FilterArrowAllocation = 548,
    FilterWireUtf8 = 549,
    FilterDeclaredWidth = 550,
    ScanOutputContract = 551,
    LocalPartitionContract = 554,
    FragmentStopRequested = 555,
    ValuesArrayBacking = 556,
    ValuesArrowAllocation = 557,
    InjectedCleanupFailure = 559,
    ObserverBackingReservation = 560,
    ObserverBackingLayout = 561,
    ObserverBackingCapacity = 562,
    ObserverInvalidCapacity = 563,
    ObserverCapacityExceeded = 564,
    ObserverBoundedRequired = 565,
    TaskControlObservedStop = 566,
    TaskControlIdentityMismatch = 567,
    SplitPlanNodeMismatch = 568,
    SplitSequenceConflict = 569,
    SplitAfterNoMore = 570,
    SplitQueueClosed = 571,
    SplitQueueBytes = 572,
    ExchangeDuplicateFrozenEdge = 573,
    ExchangeEdgeWithoutDestinations = 574,
    ExchangeDestinationClaimedTwice = 575,
    MissingTaskExecutionControl = 576,
    SplitConsumptionGateAlreadyHeld = 578,
    RootInputChunkDropPanicked = 587,
    RootInputPermitDropPanicked = 588,
    RootInputSubmissionContract = 589,
    ChildMissingFailurePort = 590,
    ChildFailurePortChanged = 591,
    FanoutMissingFailurePort = 592,
    FanoutFailurePortChanged = 593,
    DriverDroppedWithPendingExit = 594,
    WriterExitUnresolved = 595,
    RootInputExitObserverChanged = 596,
    RootInputExitObserverPanicked = 597,
    PipelineFinisherCallPanicked = 598,
    PipelineFinisherCaptureDropPanicked = 599,
    WallTimeCallPanicked = 600,
    WallTimeCaptureDropPanicked = 601,
    TerminalPublisherCallPanicked = 602,
    TerminalPublisherCaptureDropPanicked = 603,
    FragmentResourceHookPanicked = 604,
    FragmentResourceHolderDropPanicked = 605,
    ProgressCallPanicked = 606,
    ProgressCaptureDropPanicked = 607,
    CompletionRoleBindingConflict = 608,
    PreparedDrainMissingOwner = 609,
    PreparedDrainBeforeConclusion = 610,
    RootReadinessNotificationPanicked = 611,
    RootReadinessNotificationInvariant = 612,
    LogicalChangeCount = 613,
    LogicalSummaryContract = 614,
    FanoutRequiredBranchWithdrawn = 615,
    UnpivotExtension = 616,
    UnpivotExtensionAllocation = 617,
}
impl ExecutionReason {
    pub(crate) const fn code(self) -> NonZeroU16 {
        NonZeroU16::new(self as u16).unwrap()
    }
    pub(crate) const fn reason(self) -> FailureReason {
        FailureReason::new(domain::EXECUTION, self.code())
    }
}
