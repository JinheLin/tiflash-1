--------------------------- MODULE SchemaWatermark ---------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Columns, BaseColumn, Syncers, MaxTime, MaxBatch, Mode
None == "none"
ASSUME /\ BaseColumn \in Columns
       /\ MaxTime >= 4
       /\ Mode \in {"Correct", "NoClear", "SplitPublish", "MinCommit"}

VARIABLES clock, history, introduced, removed, rows, requests, owner,
          storage, decoder, badSkipped, lastEvent
vars == <<clock, history, introduced, removed, rows, requests, owner,
          storage, decoder, badSkipped, lastEvent>>

Known == {c \in Columns : introduced[c] > 0}
RemovedAt(tso) == {c \in Columns : removed[c] > 0 /\ removed[c] <= tso}
RowType == [commit_ts : 1..MaxTime, columns : SUBSET Columns]
Phases == {"Idle", "RPC", "Ready", "BeforeStore", "StoreDone", "Persisted", "EarlyGap", "Done"}
RequestType == [phase : Phases, kind : {"none", "Full", "Partial"},
                read_tso : 0..MaxTime, columns : SUBSET Columns]

Init ==
    /\ clock = 1
    /\ history = <<{BaseColumn}>>
    /\ introduced = [c \in Columns |-> IF c = BaseColumn THEN 1 ELSE 0]
    /\ removed = [c \in Columns |-> 0]
    /\ rows = {}
    /\ requests = [s \in Syncers |->
          [phase |-> "Idle", kind |-> "none", read_tso |-> 0, columns |-> {}]]
    /\ owner = None
    /\ storage = [columns |-> {BaseColumn}, tso |-> 1, disk |-> {BaseColumn},
                   cachedColumns |-> {BaseColumn}, cachedTSO |-> 1, dirty |-> FALSE]
    /\ decoder = [busy |-> FALSE, columns |-> {}, tso |-> 0, batch |-> {}]
    /\ badSkipped = {}
    /\ lastEvent = "init"

\* Ordinary DDL: each physical column ID is introduced at most once.
AddColumn(c) ==
    /\ clock < MaxTime /\ introduced[c] = 0
    /\ clock' = clock + 1
    /\ history' = Append(history, history[clock] \cup {c})
    /\ introduced' = [introduced EXCEPT ![c] = clock + 1]
    /\ lastEvent' = "add"
    /\ UNCHANGED <<removed, rows, requests, owner, storage, decoder, badSkipped>>

DropColumn(c) ==
    /\ clock < MaxTime /\ c \in history[clock]
    /\ clock' = clock + 1
    /\ history' = Append(history, history[clock] \ {c})
    /\ removed' = [removed EXCEPT ![c] = clock + 1]
    /\ lastEvent' = "drop"
    /\ UNCHANGED <<introduced, rows, requests, owner, storage, decoder, badSkipped>>

\* Delayed writers may still encode removed IDs, but never IDs not yet introduced.
Commit(encodedColumns) ==
    /\ clock < MaxTime
    /\ encodedColumns \in SUBSET Known /\ encodedColumns # {}
    /\ clock' = clock + 1
    /\ history' = Append(history, history[clock])
    /\ rows' = rows \cup {[commit_ts |-> clock + 1, columns |-> encodedColumns]}
    /\ lastEvent' = "commit"
    /\ UNCHANGED <<introduced, removed, requests, owner, storage, decoder, badSkipped>>

\* TSO acquisition and MVCC RPC response are separate; DDL may interleave.
StartRead(s) ==
    /\ requests[s].phase = "Idle"
    /\ requests' = [requests EXCEPT ![s] =
          [phase |-> "RPC", kind |-> "Full", read_tso |-> clock, columns |-> {}]]
    /\ lastEvent' = "rpc_start"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, owner, storage, decoder, badSkipped>>

CompleteRead(s) ==
    /\ requests[s].phase = "RPC"
    /\ requests' = [requests EXCEPT ![s].phase = "Ready",
                        ![s].columns = history[requests[s].read_tso]]
    /\ lastEvent' = "rpc_response"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, owner, storage, decoder, badSkipped>>

\* A partial updater can copy local TableInfo before waiting for the alter lock.
CopyLocal(s) ==
    /\ requests[s].phase = "Idle" /\ owner = None
    /\ requests' = [requests EXCEPT ![s] =
          [phase |-> "Ready", kind |-> "Partial", read_tso |-> 0, columns |-> storage.columns]]
    /\ lastEvent' = "partial_copy"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, owner, storage, decoder, badSkipped>>

Older(s) == requests[s].read_tso # 0 /\ requests[s].read_tso < storage.tso

IgnoreOlder(s) ==
    /\ requests[s].phase = "Ready" /\ owner = None /\ ~decoder.busy /\ Older(s)
    /\ requests' = [requests EXCEPT ![s].phase = "Done"]
    /\ lastEvent' = "ignore_older"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, owner, storage, decoder, badSkipped>>

BeginAlter(s) ==
    /\ requests[s].phase = "Ready" /\ owner = None /\ ~decoder.busy /\ ~Older(s)
    /\ LET split == Mode = "SplitPublish" /\ requests[s].kind = "Full"
       IN /\ owner' = IF split THEN None ELSE s
          /\ requests' = [requests EXCEPT ![s].phase = IF split THEN "EarlyGap" ELSE "BeforeStore"]
          /\ storage' = IF split
                THEN [storage EXCEPT !.tso = requests[s].read_tso, !.dirty = TRUE]
                ELSE IF Mode = "NoClear" THEN storage ELSE [storage EXCEPT !.tso = 0]
    /\ lastEvent' = "begin_alter"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, decoder, badSkipped>>

\* Deliberately broken variant: the watermark became visible outside the lock,
\* before its schema. Reacquiring later cannot undo an intervening unsafe decode.
ResumeSplit(s) ==
    /\ requests[s].phase = "EarlyGap" /\ owner = None /\ ~decoder.busy
    /\ requests' = [requests EXCEPT ![s].phase = "BeforeStore"]
    /\ owner' = s
    /\ lastEvent' = "resume_split"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, storage, decoder, badSkipped>>

ApplyStore(s) ==
    /\ requests[s].phase = "BeforeStore" /\ owner = s
    /\ storage' = [storage EXCEPT !.columns = requests[s].columns, !.dirty = TRUE]
    /\ requests' = [requests EXCEPT ![s].phase = "StoreDone"]
    /\ lastEvent' = "store_update"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, owner, decoder, badSkipped>>

Persist(s) ==
    /\ requests[s].phase = "StoreDone" /\ owner = s
    /\ storage' = [storage EXCEPT !.disk = storage.columns]
    /\ requests' = [requests EXCEPT ![s].phase = "Persisted"]
    /\ lastEvent' = "persist"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, owner, decoder, badSkipped>>

Publish(s) ==
    /\ requests[s].phase = "Persisted" /\ owner = s
    /\ storage' = [storage EXCEPT !.tso = requests[s].read_tso]
    /\ requests' = [requests EXCEPT ![s].phase = "Done"]
    /\ owner' = None
    /\ lastEvent' = "publish"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, decoder, badSkipped>>

Fail(s) ==
    /\ requests[s].phase \in {"BeforeStore", "StoreDone", "Persisted"} /\ owner = s
    /\ lastEvent' = IF requests[s].phase = "BeforeStore" THEN "fail_before_store" ELSE "fail_after_store"
    /\ requests' = [requests EXCEPT ![s].phase = "Done"]
    /\ owner' = None
    /\ UNCHANGED <<clock, history, introduced, removed, rows, storage, decoder, badSkipped>>

\* Shared decode lock prevents store updates between snapshot acquisition and decode.
AcquireSnapshot(batch) ==
    /\ owner = None /\ ~decoder.busy
    /\ batch \in SUBSET rows /\ Cardinality(batch) \in 1..MaxBatch
    /\ LET cols == IF storage.dirty THEN storage.columns ELSE storage.cachedColumns
           tso == IF storage.dirty THEN storage.tso ELSE storage.cachedTSO
       IN /\ decoder' = [busy |-> TRUE, columns |-> cols, tso |-> tso, batch |-> batch]
          /\ storage' = [storage EXCEPT !.cachedColumns = cols, !.cachedTSO = tso, !.dirty = FALSE]
    /\ lastEvent' = "snapshot"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, requests, owner, badSkipped>>

CommitTimes(batch) == {r.commit_ts : r \in batch}
Largest(batch) == CHOOSE t \in CommitTimes(batch) : \A x \in CommitTimes(batch) : x <= t
Smallest(batch) == CHOOSE t \in CommitTimes(batch) : \A x \in CommitTimes(batch) : t <= x
Covered(batch, tso) ==
    /\ tso > 0
    /\ (IF Mode = "MinCommit" THEN Smallest(batch) ELSE Largest(batch)) <= tso

Decode ==
    /\ decoder.busy
    /\ LET extras == (UNION {r.columns : r \in decoder.batch}) \ decoder.columns
           skipped == IF Covered(decoder.batch, decoder.tso) THEN extras ELSE {}
           unsafe == skipped \ RemovedAt(decoder.tso)
       IN /\ badSkipped' = badSkipped \cup unsafe
          /\ lastEvent' = IF unsafe # {} THEN "unsafe_skip"
                ELSE IF skipped # {} THEN "historical_skip"
                ELSE IF extras # {} THEN "fallback" ELSE "decode_known"
    /\ decoder' = [decoder EXCEPT !.busy = FALSE]
    /\ UNCHANGED <<clock, history, introduced, removed, rows, requests, owner, storage>>

\* Process restart discards in-flight RPCs and snapshots; watermark is rebuilt,
\* rather than persisted. Disk may contain an older or partially-persisted schema.
Restart ==
    /\ storage' = [storage EXCEPT !.columns = storage.disk, !.tso = 0,
                        !.cachedColumns = storage.disk, !.cachedTSO = 0, !.dirty = FALSE]
    /\ owner' = None
    /\ decoder' = [decoder EXCEPT !.busy = FALSE]
    /\ requests' = [s \in Syncers |-> IF requests[s].phase = "Idle" THEN requests[s]
                        ELSE [requests[s] EXCEPT !.phase = "Done"]]
    /\ lastEvent' = IF owner # None THEN "crash_during_alter"
                     ELSE IF decoder.busy THEN "crash_during_decode" ELSE "restart"
    /\ UNCHANGED <<clock, history, introduced, removed, rows, badSkipped>>

Next ==
    \/ \E c \in Columns : AddColumn(c) \/ DropColumn(c)
    \/ \E encodedColumns \in SUBSET Known : Commit(encodedColumns)
    \/ \E s \in Syncers : StartRead(s) \/ CompleteRead(s) \/ CopyLocal(s)
          \/ IgnoreOlder(s) \/ BeginAlter(s) \/ ResumeSplit(s) \/ ApplyStore(s)
          \/ Persist(s) \/ Publish(s) \/ Fail(s)
    \/ \E batch \in SUBSET rows : AcquireSnapshot(batch)
    \/ Decode \/ Restart

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ clock \in 1..MaxTime /\ Len(history) = clock
    /\ \A i \in 1..clock : history[i] \subseteq Columns
    /\ introduced \in [Columns -> 0..MaxTime] /\ removed \in [Columns -> 0..MaxTime]
    /\ rows \subseteq RowType /\ requests \in [Syncers -> RequestType]
    /\ owner \in Syncers \cup {None}
    /\ storage \in [columns : SUBSET Columns, tso : 0..MaxTime, disk : SUBSET Columns,
                     cachedColumns : SUBSET Columns, cachedTSO : 0..MaxTime, dirty : BOOLEAN]
    /\ decoder \in [busy : BOOLEAN, columns : SUBSET Columns, tso : 0..MaxTime, batch : SUBSET rows]
    /\ badSkipped \subseteq Columns

\* These are checked as invariants, never used as guards for an implementation action.
PublishedPair == storage.tso > 0 => storage.columns = history[storage.tso]
CachedPair == storage.cachedTSO > 0 => storage.cachedColumns = history[storage.cachedTSO]
DecoderPair == decoder.busy /\ decoder.tso > 0 => decoder.columns = history[decoder.tso]
NoValidColumnSkipped == badSkipped = {}
OrdinaryCommitOrder == \A r \in rows : \A c \in r.columns :
                         introduced[c] > 0 /\ introduced[c] < r.commit_ts
HistoryLifecycle == \A t \in 1..clock : history[t] =
    {c \in Columns : introduced[c] > 0 /\ introduced[c] <= t /\
                     (removed[c] = 0 \/ removed[c] > t)}

\* Reachability sentinels: expected to FAIL in dedicated witness configurations.
NoHistoricalSkip == lastEvent # "historical_skip"
NoFallback == lastEvent # "fallback"
NoFailureAfterStore == lastEvent # "fail_after_store"
NoRestart == lastEvent # "restart"
NoCrashDuringAlter == lastEvent # "crash_during_alter"
NoCrashDuringDecode == lastEvent # "crash_during_decode"
=============================================================================
