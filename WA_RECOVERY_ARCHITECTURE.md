# WhatsApp Recovery Architecture

This document records the reconnect hardening that was added after repeated production incidents where the WhatsApp session dropped while the container process remained alive.

## Problem Summary

Observed production symptoms:

- WhatsApp disconnected with errors such as `Connection Terminated` (`428`) and `Stream Errored (unknown)` (`503`).
- The container stayed alive and MCP continued to answer, but the WhatsApp layer was no longer usable.
- A manual container restart restored service.

Two distinct failure classes were identified:

1. App-state corruption
- Signals:
  - `failed to find key ... to decode mutation`
  - `failed to sync state from version`
- Mitigation:
  - `forceResync()`
  - escalate to internal restart after repeated failures

2. Socket disconnect / transport termination
- Signals:
  - `Connection Terminated`
  - `Stream Errored (unknown)`
  - other `connection.update -> close` events
- Mitigation:
  - disconnect watchdog
  - reconnect or restart based on status code / reason

## Root Cause Found In Recovery Logic

The main operational outage was not caused only by the WhatsApp disconnect itself. The reconnect path in the service had a deadlock.

Previous flow:

- `reconnect()` acquired `withLifecycleLock(...)`
- inside that locked section it called `initialize()`
- `initialize()` also tried to acquire `withLifecycleLock(...)`

This created a nested lock deadlock. In logs it looked like:

- `Starting WhatsApp reconnect`
- `Reconnect: destroying current WhatsApp client`
- `Reconnect: initializing WhatsApp client`
- then no further progress

As a result:

- the process remained alive
- `/healthz` eventually went unhealthy
- manual container restart was required

## Technical Fix

The reconnect lifecycle was changed so that initialization inside an already locked recovery path does not try to reacquire the same lifecycle lock.

Key implementation points in [`src/services/whatsapp.ts`](src/services/whatsapp.ts):

- `initializeWithinLifecycleLock()`
  - internal initialize path that assumes the lifecycle lock is already held
- `initialize()`
  - public entrypoint that still acquires the lifecycle lock
- `reconnect()`
  - now calls `initializeWithinLifecycleLock()` instead of recursively calling `initialize()`
- `restart()`
  - same change
- `forceResync()`
  - same change

## Disconnect Watchdog

Additional hardening was added for socket-level disconnects.

Behavior:

- on `connection.update -> close`, the service schedules a disconnect watchdog
- if the socket does not return to `open` in time:
  - `restart()` is used for configured codes such as `428`
  - `reconnect()` is used for less severe cases

Relevant environment variables:

- `WA_DISCONNECT_RECOVERY_DELAY_MS`
- `WA_DISCONNECT_RECOVERY_RESTART_CODES`

## Result

After the deadlock fix, production logs showed the same class of disconnects being auto-recovered:

- `428 Connection Terminated`
- `503 Stream Errored (unknown)`

Recovery sequence now completes:

- `Scheduled disconnect recovery watchdog`
- `Starting WhatsApp reconnect`
- `Reconnect flow completed`
- `connection: "open"`

This means:

- disconnects still happen at the WA/Baileys layer
- but they are no longer necessarily fatal to service availability
- the system now self-recovers without requiring routine manual container restarts

## Remaining Reality

This is not a protocol-level fix for all WA/Baileys disconnects.

What is fixed:

- recovery no longer deadlocks
- repeated disconnects can be auto-recovered

What is not guaranteed:

- that WA/Baileys transport disconnects will stop happening
- that every future disconnect pattern will be recovered by the current policy

## Operational Guidance

When investigating future incidents, check:

1. `/healthz`
2. `logs/mcp-whatsapp.log`
3. presence of:
   - `Scheduled disconnect recovery watchdog`
   - `Starting WhatsApp reconnect`
   - `Reconnect flow completed`
   - `connection: "open"`

If disconnects continue but recovery completes, the problem is transport instability, not a stuck recovery path.

If recovery stops before `Reconnect flow completed`, the recovery lifecycle needs further investigation.
