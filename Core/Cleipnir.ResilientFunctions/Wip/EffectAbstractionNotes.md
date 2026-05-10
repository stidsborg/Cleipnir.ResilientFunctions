# Effect Abstraction Notes (working)

Notes from a design conversation about Cleipnir's `Effect` / `EffectId` abstractions. Captures the critique, the strengths, and a sketched path toward replay drift detection. This is a working document — use it to resume the discussion, not as a finished design.

## Quick recap of the types

- **`EffectId`** (`Core/Cleipnir.ResilientFunctions/Domain/EffectId.cs`) is a `record EffectId(int[] Value)` — a path in a tree. `[5]`, `[5, 0, 2]`, `[-1, 0]`. `CreateChild(int)` extends the path. `IsChild` / `IsDescendant` walk the tree. Identity is hierarchy.

- **`Effect`** (`Core/Cleipnir.ResilientFunctions/Domain/Effect.cs`) is a façade over `EffectResults` (a KV store keyed by `EffectId`). Exposes:
  - **Memoization runtime** — `Capture(work)` runs once, persists, returns cached on replay.
  - **Tree-keyed KV ops** — `Upsert`, `Get`, `Clear`, `GetChildren`, `IsDirty`, `Flush`.
  - **Implicit-id allocator** — `EffectContext.CurrentContext.NextEffectId()` (AsyncLocal counter).
  - **Iteration helpers** — `ForEach`, `AggregateEach` with auto-checkpointing.
  - **Flow integration** — `RegisterQueueManager`, `RunParallelle`.

- **`EffectContext`** (`Core/Cleipnir.ResilientFunctions/Domain/EffectContext.cs`) — `AsyncLocal<EffectContext>` carrying parent EffectId + an int counter. `NextEffectId` increments and appends.

## Three soft spots in the current design

### 1. `EffectId` is type-leaky

The recent `ReservedIdPrefix = -1` work in `QueueManager` exists *because* nothing prevents user code from constructing `new EffectId([-1, 0])` and colliding with internal slots. The reservation is convention, not type-enforced.

A stronger version would distinguish user-allocated subtrees (non-negative leading int) from system-allocated subtrees, either by:
- A closed `EffectId` ctor that requires a parent + non-negative leaf, with a separate factory for system roots
- A separate type (`SystemEffectId` / `UserEffectId`) sharing serialization but not interchangeable in API surface

The current state is a band-aid.

### 2. `Effect` is two abstractions wearing one name

The class is doing five jobs: memoization runtime, tree-keyed KV store, implicit-id allocator, iteration helpers, flow integration. The tell that the layering is wrong:

- `QueueManager` and `IdempotencyKeys` use `Effect` purely as a tree-keyed KV — they bypass `Capture` because their lifecycle isn't "run once and cache the result." They have their own state machines.
- They reach past the high-level API (`Capture`) into `internal`-marked primitives (`FlushlessUpsert`, `GetChildren`, `ClearNoFlush`).

When internal subsystems consume the *low-level* primitives of a class whose *high-level* API doesn't fit them, the layering should split:

```
EffectStore     — tree-keyed KV with dirty/flush/children semantics
Effect          — thin façade: memoization on top of EffectStore + EffectContext
```

### 3. The implicit-id determinism contract is unenforced

`NextEffectId()` increments a counter on each call. Replay correctness requires the *order and structure* of `Capture` calls during replay to match the original execution exactly. Conditional branches, exception paths, threads that don't propagate the AsyncLocal — all silently desynchronize ids.

This is the same contract Temporal has, but Temporal guards it with workflow workers that fail loudly on non-determinism, sandboxed contexts, replay diagnostics. Cleipnir relies on developer discipline. Aliases (`Capture("step1", work)`) are the escape valve, but the default path is implicit and brittle.

## The strength: recursive Capture and path-based ids

Cleipnir's `Capture` is recursive — inside `Capture`, more `Capture`. Each nested call gets a child id derived from the parent's path. The EffectId tree mirrors the call graph.

This is a real strength over Temporal:

- Temporal's hierarchy is fixed-depth (workflow → activity). Calling activity-from-activity isn't allowed. The history is essentially flat.
- Cleipnir's hierarchy is arbitrary depth. Path-based identity carries structure that flat sequence-based identity can't.

What this enables:

- **Refactoring a method into helpers preserves ids.** As long as the helper is called from the same site, its internal Captures still live under the same parent path. The tree deepens; positions don't shift.
- **Saga primitives compose as plain C#.** You can write `RetryWithBackoff`, `RaceTwo`, your own `ForEach` as ordinary methods that call `Capture` internally. Temporal needs framework-level workflow APIs for each.
- **Abstraction levels are free.** A `BookingFlow` can call helper methods that themselves Capture. No `ChildWorkflow` ceremony, no separate registration. The tree carries the hierarchy.

This is not just "looseness." It's a more expressive identity model — paths carry tree structure that sequence-based ids don't.

## The cost: silent drift on refactor + in-flight flows

The same recursive/positional design that gives compositional power gives no drift detection. Concretely, on replay:

- **Capture inserted before existing one.** New Capture takes the id that previously belonged to the next one. Returns the previous Capture's stored result as if it were its own. Silent corruption — wrong data flows into your code.
- **Capture removed.** Stored result becomes dead weight. Successive Captures cache-hit on earlier-than-intended results.
- **Capture body changed.** Body never runs (cached id matches). Bug fix doesn't apply to in-flight flows.
- **Conditional branch changed.** Replay takes the new branch, allocates ids that don't match storage. Mix of cache hits in some places, misses in others.

Aliases mitigate for explicitly-tagged Captures. Default Captures are positional and brittle.

Strength and cost are the same coin: looseness = no rigid sequence contract = composes naturally = drift undetectable. Strictness (Temporal) = rigid sequence contract = composes awkwardly = drift detectable. The interesting question is whether Cleipnir can exploit its path-based ids to add detection without giving up the loose programming model.

## The insight: path-based ids carry more structure than Temporal can

Temporal's history is one-dimensional. Drift detection there means "the next event matches the next expected event." Sequential.

Cleipnir's effect tree has *shape* — branching factor, depth, sibling counts per parent. If a replayed flow's tree topology doesn't match the stored topology, that's a stronger drift signal than counter-position matching could give. You're comparing structures, not sequences.

This is the lever for adding safety without losing ergonomics.

## Drift detection: how to do it in C#

### Options considered

**Option 1: `[CallerFilePath]` / `[CallerLineNumber]` / `[CallerMemberName]` (recommended baseline)**

Compiler-injected at call site. Zero developer overhead. Sketch:

```csharp
using System.Runtime.CompilerServices;

public Task Capture(
    Func<Task> work,
    ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce,
    [CallerFilePath]   string filePath   = "",
    [CallerLineNumber] int    lineNumber = 0,
    [CallerMemberName] string memberName = "")
{
    var siteHash = HashCaptureSite(filePath, lineNumber, memberName);
    return InnerCapture(..., siteHash);
}
```

Store `SiteHash` alongside each persisted effect. On replay:

```csharp
var stored = effectResults.GetOrValueDefault(effectId);
if (stored != null && stored.SiteHash != siteHash)
{
    Logger.Warn($"Capture site moved for {effectId}: stored at {stored.SiteHash:X}, now at {siteHash:X}");
    // policy: warn, fail, or fall through to alias
}
```

Gotchas:

- **`[CallerFilePath]` is the build-machine absolute path.** Different machines produce different hashes for the same line. Fix: `<DeterministicSourcePaths>true</DeterministicSourcePaths>` + SourceLink, or hash only `Path.GetFileName(filePath) + memberName + lineNumber`.
- **Line numbers are noisy.** Adding a comment shifts every following line. Mitigate by hashing `(filename, memberName, ordinalPositionWithinMember)` instead of raw line — but that ordinal isn't computable at runtime without source-gen.
- **Plumbing surface.** `Effect.cs` has ~16 public Capture overloads. `[CallerXxx]` only injects at the outermost site, so each public overload needs the params. Bundle them into a `CaptureSite` struct and thread that down.

**Option 2: `[CallerArgumentExpression]` (C# 10+)**

Captures the source text of an argument. `Capture(() => SendEmail(user))` passes `"() => SendEmail(user)"` as a string. Survives line-number changes, formatting changes (mostly). Sensitive to renames within the lambda.

Combine with Option 1: hash `(memberName, argumentExpression)` — "what work, in which method." Decent middle ground without source-gen.

**Option 3: Roslyn source generator**

Walk every `Capture` call site at build time, parse the lambda's syntax tree, hash a normalized form (whitespace stripped, trivia removed), emit a constant per site. Strongest stability, but real machinery and IDE-integration headaches. Don't start here.

**Option 4: Expression trees** — reject. `Expression<Func<Task>>` can't contain statements or `await`. Non-starter for saga code.

### Recommendation

Start with Option 1 (`[CallerXxx]` attributes). Treat the hash as a soft signal — log warnings, don't throw. Aliases skip the check (they have stable identity by definition). If line-number noise becomes a real issue, layer in `[CallerArgumentExpression]`. Source generators are the principled answer if Phase 1 + 2 still aren't enough.

### Don't forget the abandoned-effect signal

Drift detection by hash only catches drift for ids that already exist in storage. New Captures inserted at fresh positions just write new effects with the new hash — no alarm fires. You also need a complementary check: **on replay completion, are there stored effects whose ids the saga never asked for?** Those are abandoned — strong signal a Capture was deleted between deploys. Surface in the same drift report.

## Storage cost

### Naïve cost (per-effect hash column)

8-byte hash (4-byte is too narrow — birthday collisions ~6% at 10k sites; 8 is comfortable). Per-row in PostgreSQL, SQL Server, MariaDB: 8 bytes raw, 2–4 bytes after page compression.

As fraction of row size:
- `Mark()` (no payload): +16–25%
- Typical Capture with serialized result: +1.5–8%
- Larger result (DTOs, lists): <0.1%

Disproportionately felt by tiny effects. Not catastrophic — 10M effects × 8 bytes = ~20–40 MB compressed.

### Structural insight: hashes are properties of call sites, not effects

The hash of `Capture` at line 42 of `BookingSaga.cs` is the same every execution. `ForEach` over 10k items at one site stores the same 8 bytes 10k times. That's a modeling problem, not a storage problem:

- **Code scale**: bounded by lines of Capture-using code (hundreds in a mature codebase). Static.
- **Data scale**: bounded by traffic (millions+). Grows.

Per-effect storage scales metadata by the data axis. It should scale by the code axis.

### Real alternatives

**A) Site interning table** — clean separation:

```sql
CREATE TABLE capture_sites (
    site_id     INT PRIMARY KEY,
    site_hash   BIGINT NOT NULL UNIQUE,
    file_name   TEXT,
    member_name TEXT,
    line_number INT
);

ALTER TABLE effects ADD COLUMN site_id INT REFERENCES capture_sites(site_id);
```

4-byte FK per effect instead of 8-byte hash. Capture sites table is tiny (KB), populated lazily on first hit per site. Bonus: queryable provenance ("which sites haven't been hit in 30 days?"). One extra writeable code path (insert-if-not-exists per unique site, then cached).

**B) Per-flow execution shape (Temporal-ish)**

Single rolling hash per flow. Each Capture mixes `(siteHash, depth)` into the flow's running fingerprint. Compare at flush points / suspends.

8 bytes per flow, not per effect. 10,000× reduction for big-fanout sagas. Loses precision: detects "something drifted in this flow" without knowing which Capture. XOR-style mixing is order-invariant; want a Merkle-ish structure for stronger detection. Useful as a coarse boundary check, not per-step.

**C) Sampling** — reject. Drift detection is a safety feature, not a telemetry one.

### Recommended phasing

1. **Per-effect 8-byte hash, no interning.** Ship the simplest thing. Learn whether the detection is *useful* before optimizing how it's stored.

2. **If a customer profile shows the cost biting** (workloads dominated by tiny Marks, or scales where 5% storage growth matters): add the interning table (Option A). Backwards-compatible — populate `site_id` lazily on read, never rewrite old rows.

3. **Layer Option B on top later** if you want stronger boundary guarantees. Per-effect and per-flow detection catch different drift modes; not mutually exclusive.

### Other costs that aren't bytes

- **Write amplification**: not affected — number of writes unchanged, only their size. Latency dominated by round-trip count, not byte volume. Free.
- **Hot row contention**: don't index `site_hash`. Witness column on read-by-EffectId. Free.
- **Schema migration across three stores**: ceremony, not cost. PostgreSQL + SQL Server + MariaDB each need migration + SQL builder updates.

## Bigger picture

The most interesting future-of-Cleipnir question this raises: **can the path-as-tree property be exploited for drift detection in a way Temporal architecturally can't?** If yes, you get Temporal-level safety with Cleipnir-level ergonomics. That's a real moat, and it's specifically enabled by the recursive Capture design.

The phased plan above is the cheapest viable path:

1. `[CallerXxx]` + per-effect 8-byte hash. Cheap, useful, limited.
2. Site interning. Same detection, scales right.
3. Per-flow execution shape. Coarse, complementary, catches whole-flow corruption.
4. Source generator with normalized syntax-tree hashes. Long-term safety story.

Each step is a backwards-compatible addition. None require changing the user-facing programming model.

## Open questions to resume on

- Should drift detection be **warning-only** by default, with a strict mode opt-in? Strict mode would fail the flow on hash mismatch. Most users probably want warnings during refactor; production might want strict on critical sagas.
- How does drift detection interact with **versioned sagas** (the `getVersion` problem)? Aliases are the current answer. Is that enough, or do we want explicit `Capture.Version("v2", () => ...)` semantics?
- The **abandoned-effect detector** (effects whose ids were never asked for during replay) — does it run at flow completion, periodically, or via a separate sweep? Probably flow completion, but worth thinking about idle flows that never complete.
- For the **EffectStore vs Effect split** (soft spot 2): is this worth doing as a refactor in isolation, or only as part of a larger cleanup? `QueueManager` and `IdempotencyKeys` are the only internal consumers today; not a wide blast radius. Probably feasible.
- For **`EffectId` user/system separation** (soft spot 1): does the type-level fix justify the migration cost? The convention is now documented in `QueueManager.ReservedIdPrefix`. May be enough until a third party tries to extend the framework.
- **Cost of deterministic source paths**: SourceLink + `<DeterministicSourcePaths>` is straightforward to enable in csprojs but interacts with debugging, code coverage tools. Verify there's no friction before assuming it's free.

## Files referenced

- `Core/Cleipnir.ResilientFunctions/Domain/EffectId.cs`
- `Core/Cleipnir.ResilientFunctions/Domain/Effect.cs`
- `Core/Cleipnir.ResilientFunctions/Domain/EffectContext.cs`
- `Core/Cleipnir.ResilientFunctions/Queuing/QueueManager.cs`
- `Core/Cleipnir.ResilientFunctions/Queuing/IdempotencyKeys.cs`
