import gc
from contextvars import Context
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "BridgeEmulator"))

import HueObjects
from flask import Flask
from services import eventStreamer


RID = "11111111-1111-4111-8111-111111111111"
EVENT_ID = "33333333-3333-4333-8333-333333333333"


def message(index=0, kind="delete"):
    return {"id": EVENT_ID, "type": kind,
            "data": [{"id": RID, "type": "scene",
                      "metadata": {"name": "PRIVATE_PAYLOAD_CANARY", "index": index}}]}


class ExistingRouteRegressionTests(unittest.TestCase):
    def test_connected_route_does_not_repeat_the_same_publication(self):
        app = Flask(__name__)
        with patch.object(eventStreamer, "KEEPALIVE_S", 0), \
                app.test_request_context("/eventstream/clip/v2"):
            response = eventStreamer.streamV2Events()
            self.addCleanup(response.close)
            frames = iter(response.response)
            self.assertEqual(next(frames), ": hi\n\n")
            HueObjects.StreamEvent(message())
            self.assertIn("data: [", next(frames))
            self.assertEqual(next(frames), ": keepalive\n\n")


class EventStreamTests(unittest.TestCase):
    def setUp(self):
        self.journal = HueObjects.EventJournal(max_events=3, max_bytes=4096)
        patcher = patch.object(HueObjects, "events", self.journal)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.app = Flask(__name__)
        self.app.register_blueprint(eventStreamer.stream)

    def open(self, last_id=None):
        headers = {"hue-application-key": "PRIVATE_APP_KEY_CANARY"}
        if last_id is not None:
            headers["Last-Event-ID"] = last_id
        context = Context()

        def connect():
            with self.app.test_request_context("/eventstream/clip/v2", headers=headers):
                response = eventStreamer.streamV2Events()
                frames = iter(response.response)
                self.assertEqual(next(frames), ": hi\n\n")
            return response, frames

        response, frames = context.run(connect)

        def close():
            context.run(response.close)

        def read():
            while True:
                try:
                    frame = context.run(next, frames)
                except StopIteration:
                    return
                yield frame

        self.addCleanup(close)
        return SimpleNamespace(close=close), read()

    def assert_entry(self, frames, entry):
        self.assertEqual(next(frames), f"id: {entry.sse_id}\ndata: {entry.data}\n\n")

    def test_independent_readers_order_snapshot_and_disconnect(self):
        response_a, a = self.open()
        response_b, b = self.open()
        original = message(1)
        first = HueObjects.StreamEvent(original)
        original["data"][0]["metadata"]["name"] = "MUTATED"
        decoded = first.message
        decoded["data"][0]["metadata"]["index"] = 99
        self.assertEqual(first.message["data"][0]["metadata"]["index"], 1)
        second = HueObjects.StreamEvent(message(2))
        for frames in (a, b):
            self.assert_entry(frames, first)
            self.assert_entry(frames, second)
        self.assertIn("PRIVATE_PAYLOAD_CANARY", first.data)
        with self.assertLogs(eventStreamer.logging, level="INFO") as logs:
            response_a.close()
        self.assertIn("reason=disconnect", "\n".join(logs.output))
        third = HueObjects.StreamEvent(message(3))
        self.assert_entry(b, third)
        self.assertEqual([entry.seq for entry in self.journal.wait_after(0, 0).entries], [1, 2, 3])

    def test_replay_and_oldest_minus_one_boundary(self):
        entries = [HueObjects.StreamEvent(message(n)) for n in range(4)]
        response, frames = self.open(entries[0].sse_id)
        for entry in entries[1:]:
            self.assert_entry(frames, entry)
        live = HueObjects.StreamEvent(message(5))
        self.assert_entry(frames, live)
        response.close()
        response, frames = self.open(entries[2].sse_id)
        self.assert_entry(frames, entries[3])
        self.assert_entry(frames, live)
        response.close()
        self.assertEqual(self.journal.resume(entries[-1].sse_id).start, "replay")

    def test_retained_scene_delete_reaches_connected_real_route(self):
        import test_smart_scene as fixtures
        from HueObjects import Scene, SmartScene
        config = fixtures.CONFIG
        saved = dict(config)
        self.addCleanup(lambda: (config.clear(), config.update(saved)))
        config.update({"apiUsers": {"test": SimpleNamespace(username="test")},
                       "lights": {}, "groups": {}, "scenes": {}, "smart_scene": {}})
        for rtype, store, cls in (("scene", "scenes", Scene.Scene),
                                  ("smart_scene", "smart_scene", SmartScene.SmartScene)):
            with self.subTest(rtype=rtype):
                target = cls({"name": "PRIVATE_SCENE_CANARY", "id_v1": "7",
                              "id_v2": RID, "owner": SimpleNamespace(username="test")})
                config[store]["7"] = target
                response, frames = self.open()
                start = self.journal.head()
                with patch.object(fixtures.v2restapi.configManager.bridgeConfig, "save_config"), \
                        patch.object(fixtures.v2restapi.configManager.bridgeConfig, "mark_dirty"), \
                        self.app.test_request_context(method="DELETE",
                                                      headers={"hue-application-key": "test"}):
                    deleted = self.app.make_response(
                        fixtures.v2restapi.ClipV2ResourceId().delete(rtype, RID))
                self.assertEqual(deleted.status_code, 200)
                entry, = self.journal.wait_after(start, 0).entries
                self.assertEqual(entry.message["data"], [{"id": RID, "type": rtype}])
                self.assert_entry(frames, entry)
                del target
                gc.collect()
                self.assertEqual(self.journal.head(), entry.seq)
                response.close()

    def test_slow_reader_gap_closes_fast_reader_unaffected(self):
        _, slow = self.open()
        _, fast = self.open()
        for n in range(4):
            entry = HueObjects.StreamEvent(message(n))
            self.assert_entry(fast, entry)
        with self.assertLogs(eventStreamer.logging, level="WARNING") as logs:
            self.assertIn("stream gap", next(slow))
            with self.assertRaises(StopIteration):
                next(slow)
        self.assertIn("missed=1", "\n".join(logs.output))
        live = HueObjects.StreamEvent(message(4))
        self.assert_entry(fast, live)

    def test_resume_classifications_and_live_after_gap(self):
        entries = [HueObjects.StreamEvent(message(n)) for n in range(5)]
        cases = [(None, "live"), ("", "invalid_id"),
                 (entries[0].sse_id, "gap_evicted"),
                 ("0:1", "gap_epoch"),
                 (f"{self.journal.epoch}:999", "invalid_id"),
                 ("PRIVATE_HEADER_CANARY", "invalid_id"),
                 ("9" * 200, "invalid_id")]
        for header, classification in cases:
            with self.subTest(header=classification):
                resume = self.journal.resume(header)
                self.assertEqual(resume.start, classification)
                self.assertEqual(resume.cursor, self.journal.head())
                response, frames = self.open(header)
                if classification != "live":
                    self.assertIn("resume gap", next(frames))
                live = HueObjects.StreamEvent(message(6))
                self.assert_entry(frames, live)
                response.close()

    def test_byte_eviction_and_newest_oversized_entry(self):
        journal = HueObjects.EventJournal(max_events=10, max_bytes=350)
        first = journal.publish(message(1))
        second = journal.publish(message(2))
        batch = journal.wait_after(0, 0)
        self.assertGreater(batch.missed, 0)
        self.assertEqual(journal.wait_after(first.seq, 0).entries, (second,))
        large = message(3)
        large["data"][0]["large"] = "x" * 500
        with self.assertLogs(HueObjects.logging, level="WARNING") as logs:
            oversized = journal.publish(large)
        self.assertIn("oversized", "\n".join(logs.output))
        self.assertEqual(journal.wait_after(second.seq, 0).entries, (oversized,))

    def test_waiting_real_generator_wakes_on_publication(self):
        waiting = threading.Event()
        original = self.journal.wait_after
        result, failures = [], []

        def wait_after(cursor, timeout):
            waiting.set()
            return original(cursor, timeout)

        def consume():
            try:
                response, frames = self.open()
                result.append(next(frames))
                response.close()
            except Exception as error:
                failures.append(error)

        with patch.object(self.journal, "wait_after", side_effect=wait_after):
            thread = threading.Thread(target=consume)
            thread.start()
            self.assertTrue(waiting.wait(timeout=5))
            entry = HueObjects.StreamEvent(message(1))
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(result, [f"id: {entry.sse_id}\ndata: {entry.data}\n\n"])

    def test_keepalive_and_monotonic_lifetime(self):
        with patch.object(eventStreamer, "KEEPALIVE_S", 0), \
                patch.object(eventStreamer, "monotonic", side_effect=[0, 1, 301]):
            _, frames = self.open()
            self.assertEqual(next(frames), ": keepalive\n\n")
            with self.assertRaises(StopIteration):
                next(frames)

    def test_recursive_entry_allocation_preserves_unique_order(self):
        constructor = HueObjects.StreamEntry
        recursive = False

        def allocate(*args, **kwargs):
            nonlocal recursive
            if not recursive:
                recursive = True
                HueObjects.StreamEvent(message(1))
            return constructor(*args, **kwargs)

        with patch.object(HueObjects, "StreamEntry", side_effect=allocate):
            outer = HueObjects.StreamEvent(message(2))
        entries = self.journal.wait_after(0, 0).entries
        self.assertEqual([entry.seq for entry in entries], [1, 2])
        self.assertEqual([entry.message["data"][0]["metadata"]["index"]
                          for entry in entries], [1, 2])
        self.assertEqual(outer.seq, 2)

    def test_epoch_does_not_use_second_resolution(self):
        with patch.object(HueObjects, "time_ns", side_effect=[1000000001, 1000000002]):
            first = HueObjects.EventJournal()
            second = HueObjects.EventJournal()
        entry = first.publish(message())
        self.assertEqual(second.resume(entry.sse_id).start, "gap_epoch")
        self.assertRegex(entry.sse_id, r"^[0-9]+:[0-9]+$")

    def test_info_correlation_logs_do_not_leak_payloads_or_headers(self):
        with self.assertLogs(HueObjects.logging, level="INFO") as publish_logs, \
                self.assertLogs(eventStreamer.logging, level="INFO") as stream_logs:
            response, frames = self.open("PRIVATE_HEADER_CANARY")
            self.assertIn("resume gap", next(frames))
            entry = HueObjects.StreamEvent(message())
            self.assert_entry(frames, entry)
            response.close()
        text = "\n".join(publish_logs.output + stream_logs.output)
        for value in (RID, EVENT_ID, entry.sse_id, "connection=", "yield_to_wsgi"):
            self.assertIn(value, text)
        for value in ("PRIVATE_PAYLOAD_CANARY", "PRIVATE_APP_KEY_CANARY",
                      "PRIVATE_HEADER_CANARY", "rendered=true"):
            self.assertNotIn(value, text)
