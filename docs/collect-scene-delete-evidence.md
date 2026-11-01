# Collect scene deletion evidence

Use these steps to capture a deletion attempt without exporting bridge
configuration or app credentials.
The [fix explanation](scene-delete-events.md) describes the causes and limits.

## Capture a live attempt after deployment

The new entries appear at INFO level. Do not enable DEBUG solely for this report.
These entries require an image built with the notification changes.
The earlier smart-scene storage fix alone does not contain them.

1. Record the image reference, source commit, Hue app version, and phone platform.
2. Delete one scene in the app. Record whether it disappears without refresh
   and whether the app displays an error.
3. Capture the following short, filtered log window. Replace `diyhue` if your
   container has a different name.

```sh
docker logs --since 10m diyhue 2>&1 |
	awk -F ' - ' '
		$2 ~ /^(HueObjects|flaskUI.v2restapi|services.eventStreamer)$/ &&
		$4 ~ /^(INFO|WARNING|ERROR)$/ &&
		$5 ~ /^(scene (delete|delete_published)|event (published|oversized)|sse (open|yield_to_wsgi|gap|close)) / {
			print
		}' > diyhue-scene-delete.log
```

4. Inspect `diyhue-scene-delete.log` before attaching it.
   Use the repository's [bug-report template](../.github/ISSUE_TEMPLATE/bug-report.yml).
   Include the expected result, observed result, and reproduction steps.

The new messages omit names, app keys, client keys, IPs, raw headers, and event
bodies. They retain resource UUIDs to correlate requests with events.
If you redact UUIDs, replace each UUID consistently across the file.
Do not attach unfiltered Docker logs or a debug archive without inspecting them.
Other bridge log messages can contain information that these new entries omit.

## Trace a deletion through the logs

Find `scene delete` with the resource UUID in `rid`.
Status 200 means the handler returned success.
Status 404 means the scene was already absent or unknown.
`save_call=returned` means the save call returned, not that it wrote the file.

Match `event_id`, `seq`, and `sse_id` across `scene delete_published`,
`event published`, and `sse yield_to_wsgi`.
Each `connection` identifies one SSE connection within the process.
`source=v2` records explicit deletion.
`source=finalizer` records the destructor fallback.

`sse yield_to_wsgi` records a frame attempt.
`note=attempted_not_received` explicitly avoids claiming app receipt or display.
Publication can appear before the REST success log because the save follows it.
`sse gap` records a slow reader that missed retained history.
An `sse open` with `start=gap_evicted` or `start=gap_epoch` records an unavailable
replay window. `start=invalid_id` records an unusable header without logging it.

The following example uses synthetic identifiers and omits the logger prefix:

```text
scene delete_published resource=smart_scene rid=11111111-1111-4111-8111-111111111111 seq=8 sse_id=1000000001:8 event_id=33333333-3333-4333-8333-333333333333 source=v2 status=published
sse yield_to_wsgi connection=1 seq=8 sse_id=1000000001:8 event_id=33333333-3333-4333-8333-333333333333 type=delete resource=smart_scene/11111111-1111-4111-8111-111111111111 status=yielded note=attempted_not_received
scene delete resource=smart_scene rid=11111111-1111-4111-8111-111111111111 source=v2 seq=8 sse_id=1000000001:8 event_id=33333333-3333-4333-8333-333333333333 status=200 save_call=returned
scene delete resource=smart_scene rid=11111111-1111-4111-8111-111111111111 source=v2 status=404
```

## Reproduce without touching a live bridge

Run from the fork root with Docker installed.
The test Dockerfile layers source over a pinned image with existing dependencies.
It is not a rebuild of the official multiarchitecture image.

```sh
docker build --network none --pull=false \
	-t diyhue/core:scene-delete-events-test \
	-f .build/Dockerfile.scene-delete-events-test .

docker run --rm --network none \
	-v "$PWD:/source:ro" -e PYTHONPATH=/source/tests -w /tmp \
	diyhue/core:scene-delete-events-test \
	python3 -m unittest discover -s /source/tests -v
```

If the pinned base image is not cached, pull the digest in the test Dockerfile
before building. The suite currently runs 188 tests.

The server below adds test-only routes and a synthetic app key.
Run it only in the disposable container with `--network none`.
Do not expose those routes on a live bridge or copy live configuration into it.
The container stores its synthetic configuration under `/tmp`.

```sh
docker run -d --name diyhue-scene-events-check --network none \
	-v "$PWD/tests:/tests:ro" \
	diyhue/core:scene-delete-events-test \
	python3 /tests/scene_delete_events_server.py

docker exec diyhue-scene-events-check python3 /tests/scene_delete_events_http.py run
docker restart --time 2 diyhue-scene-events-check
docker exec diyhue-scene-events-check python3 /tests/scene_delete_events_http.py restart
docker restart --time 2 diyhue-scene-events-check
docker exec diyhue-scene-events-check python3 /tests/scene_delete_events_http.py restart-empty

docker stop --time 2 diyhue-scene-events-check
docker rm diyhue-scene-events-check
```

Expect success messages for both scene types, identical frames for two readers,
404 retries, replay, persistence, and two restarts.
The test fails if those results do not hold. If a command fails, stop and remove
the named test container before retrying.
