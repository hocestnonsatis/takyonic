# Smart Client Session SQL (Faz 4D)

**Date:** 2026-08-08  
**Status:** Done

## Goal

Let gRPC Smart Client callers run full Volcano SQL (JOIN, aggregates, DDL, …)
without a second planner on the client.

## Non-goals

- Re-implement CBO/Volcano inside `TakyonicClient`
- Expand the narrow `execute_sql` façade (stays INSERT / single-table SELECT /
  pk UPDATE|DELETE with `pgwire only` errors)
- Multi-RPC session transactions on gRPC (no durable client session id yet)

## Mechanism

1. New `ClientService.ExecuteSessionSql(sql)` RPC (leader-only).
2. Leader builds an ephemeral [`SessionState`](../../../src/pg.rs) on the local
   engine and runs `SessionState::execute_sql`.
3. Response carries command tag, encoded `Record` rows, column order, optional
   affected count.
4. [`TakyonicClient::execute_session_sql`](../../../src/client.rs) routes via
   existing NotLeader rediscover / retry.

## Surfaces

| API | Role |
|-----|------|
| `execute_sql` | Narrow RPC (unchanged) |
| `execute_session_sql` | Full Session/Volcano on leader |
| pgwire | Unchanged |

## Acceptance

- 3-node cluster: Smart Client `execute_session_sql` JOIN (or GROUP BY) returns
  expected rows after CREATE/INSERT via the same API.
- Narrow `execute_sql("… JOIN …")` still returns `pgwire only`.
