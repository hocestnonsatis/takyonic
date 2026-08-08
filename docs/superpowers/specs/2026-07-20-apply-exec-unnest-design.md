# Design: ApplyExec / Correlated Subquery Unnest

**Date:** 2026-07-20 (status refresh 2026-08-03)  
**Status:** `PhysicalPlan::Apply` + session EXISTS e2e **DONE**; correlated equi unnest + true streaming Apply **remaining**.

## Goal

Stop O(N×re-optimize) Filter-inline for correlated predicates: prefer unnest to SemiJoin; residual via ApplyExec.

## Decisions

1. **Hybrid:** equi correlated `IN`/`EXISTS` → `HashSemiJoin` / Semi when safe; else `Apply`.
2. **ApplyExec (Volcano):** outer pull → bind `OuterRef` → eval predicate → emit (streaming preferred).
3. Uncorrelated `IN` already unnests (`SubqueryUnnestingRule`).

## Remaining gaps

| Gap | Notes |
|-----|--------|
| Correlated equi-IN/EXISTS unnest | `try_subquery_unnest` only `correlated: false` |
| Streaming Apply output | `ApplyExec::drive` still buffers all kept rows |
| Large-N bench | Optional regression harness |

## Non-goals

- Full decorrelation of nested OuterRef depth >1 in first cut
- Changing uncorrelated HashSemiJoin path
