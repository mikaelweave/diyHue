# Scene deletion fixes

Scene deletion has two separate failure modes. A DELETE can fail to remove a
smart scene, or the bridge can remove a scene without notifying the Hue app.
These changes address both failures without changing resource UUIDs.

## UUID requests and storage keys

Smart scenes have a V2 UUID but live in `bridgeConfig["smart_scene"]` under
numeric-string V1 keys. The old V2 DELETE fallback indexed that dictionary with
the UUID and raised `KeyError`.

Commit `d18470dad232e19f763f4d843f82e8c231ecbc7b` adds
`SmartScene.getObjectPath()` and requests a smart-scene save after deletion.
[mikaelweave/diyHue#1](https://github.com/mikaelweave/diyHue/pull/1)
contains this first fix. An isolated HTTP test reproduced the original 500.
The fixed version removed the correct scene, preserved its sibling, and kept
the deletion across restart, including deletion of the final smart scene.

That image was deployed before the notification changes. The user's subsequent
test produced four smart-scene and fourteen ordinary-scene DELETE responses
with status 200. Three repeated deletes returned 500. Refreshing the app cleared
the transient errors. This observation proves successful deletion, not prompt
app notification.

## Logical deletion and event delivery

`Scene.__del__` and `SmartScene.__del__` previously published deletion events.
Object destruction is later than logical deletion if another caller retains
the object. An isolated test held a real smart-scene object across successful
DELETE. The event appeared only after the test released that reference.

The old `messageBroker()` also cleared one shared event list periodically.
SSE readers polled that list without their own position. An isolated execution
showed a connected reader missing an event after the broker cleared the list.
Repeated polls could also emit the same event again.

The follow-up changes replace both mechanisms:

- `AnnouncesDeletion.announce_deleted()` publishes after V2 removes the scene,
  while the DELETE handler still holds the object. The inherited destructor is
  a fallback for V1 deletion and cascades. Its one-time flag prevents another
  event when the V2-deleted object is later destroyed.
- `deleteScene()` reads the authoritative `scenes` or `smart_scene` dictionary
  rather than trusting a cached weak reference. It invalidates the cache and
  returns the existing V2 not-found body with HTTP 404 for missing or repeated
  scene deletes. A lock coordinates concurrent V2 scene deletes.
- `EventJournal` replaces the shared list and broker thread. `StreamEvent()`
  snapshots the JSON payload once. Each SSE generator owns an integer cursor
  and reads entries in publication order without removing another reader's data.

The SSE response keeps the `: hi` greeting, numeric `epoch:sequence` IDs, and
one-event JSON array. The epoch uses nanosecond resolution to distinguish
restarts in the same second. A retained `Last-Event-ID` resumes after that event.
The journal wakes waiting readers instead of relying on periodic polling.

Per-reader queues were considered but rejected. One journal shares immutable
entries without a subscriber registry, queue duplication, or disconnect cleanup
in the event store. Only the SSE generator holds its reader cursor.

## Limits and failure reporting

The journal retains at most 1024 entries with a 4 MiB serialized-data budget.
It keeps a single newest oversized entry and logs a warning rather than adding
a size exception to existing publishers. The byte budget is therefore soft.
Python object overhead and batches held by active generators are additional memory.

If a reader falls behind retained history, the stream logs the missed count,
emits an SSE comment, and closes. An evicted, previous-process, or invalid
`Last-Event-ID` logs its classification and starts live from the current head.
Neither path invents a Hue resource event or claims to restore missing history.
The client may need to reload resources through REST.

Streams send keepalive comments after 15 seconds without an event and expire
after approximately 300 seconds, preserving a bounded connection lifetime.
Closing one stream does not discard events for other readers.

The DELETE handler publishes before its existing nonblocking `save_config()`
call. `save_call=returned` does not prove a disk write. The save can skip lock
contention or an empty overwrite protected by a `.corrupt` backup.
Write errors still propagate, with `save_call=failed` in the new log.
The isolated test separately reads YAML and restarts the server to verify
normal persistence.

The deletion lock coordinates V2 scene DELETE requests, not V1 writers,
creation, every cache reader, or configuration transactions. Other resource
deletion paths are unchanged. Hue app receipt, rendering, reconnect headers,
and automatic refresh after a gap still require a live app test.
This follow-up has not been deployed to the user's bridge.

## Reproduction and evidence

Commit `844cf85c52bb68b3804b9a68395a327cd3ee3fc4` records failing regressions
before the production changes. The retained-object tests observed no immediate
delete event. The real SSE generator repeated a publication.
Missing-`EventJournal` errors in the new contract tests are not behavioral evidence.

The focused regressions cover both scene types, finalizer fallback, repeat and
concurrent deletion, two readers, replay, eviction gaps, payload snapshots,
disconnects, and log privacy. The full existing unittest suite passed 188 tests.

The synthetic HTTP driver verifies two connected consumers receiving the same
delete event while the object remains retained. It checks that retries return
404, finalization adds no duplicate, and reconnect replays the next deletion.
YAML checks and two server restarts verify sibling preservation and empty
collections without scene resurrection.

[Collect scene deletion evidence](collect-scene-delete-evidence.md) contains
the executable commands and the log fields for a later bug report.
