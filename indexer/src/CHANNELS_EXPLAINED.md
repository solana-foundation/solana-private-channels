# Channel Architecture Decisions

> **Note**: All channels use `tokio::sync::mpsc` for implementation simplicity. The decisions below reflect the **logical topology** (how many producers/consumers we expect) even though the underlying implementation is MPSC.

## INDEXER MODE

### Channel 1: Datasource → Processor
**Decision**: ✅ **MPSC** (Multiple producers, Single consumer)
**Implementation**: `tokio::sync::mpsc`

**Why**: Multiple producers (backfill + live datasource + multiple datasources), single processor for slot ordering. Checkpoint requires sequential processing to maintain monotonic slot tracking.

---

### Channel 2: Processor → Checkpoint
**Decision**: ✅ **SPSC** (Single producer, Single consumer)
**Implementation**: `tokio::sync::mpsc` (used as SPSC)

**Why**: Single processor → single checkpoint writer. Perfect 1:1 topology.

---

## OPERATOR MODE

### Channel 3: Fetcher → Processor
**Decision**: ✅ **MPSC** (Multiple producers, Single consumer)
**Implementation**: `tokio::sync::mpsc`

**Why**: Multiple fetchers, single processor. MPMC impossible due to rotation coordination at nonce boundaries - multiple processors would race to send RotateBitmap transactions.

---

### Channel 4: Processor → Sender
**Decision**: ✅ **SPSC** (Single producer, Single consumer)
**Implementation**: `tokio::sync::mpsc` (used as SPSC)

**Why**: Single processor to single sender. Perfect 1:1 topology. Sender is single-threaded so a rotation never overtakes an in-flight release.

---

### Channel 5: Sender → Storage
**Decision**: ✅ **SPMC** (Single producer, Multiple consumers)
**Implementation**: `tokio::sync::mpsc` (used as SPMC - requires spawning multiple storage workers)

**Why**: Single sender → multiple storage workers for parallel DB writes. Allows scaling if DB becomes bottleneck.

---
