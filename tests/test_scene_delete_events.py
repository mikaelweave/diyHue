import gc
import threading
import weakref
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_smart_scene as fixtures
import HueObjects
from HueObjects import Scene, SmartScene


CONFIG = fixtures.CONFIG
v2restapi = fixtures.v2restapi
RID = "11111111-1111-4111-8111-111111111111"
SIBLING = "22222222-2222-4222-8222-222222222222"


class SceneDeletionEventsTests(unittest.TestCase):
    def setUp(self):
        gc.collect()
        fixtures.SmartSceneTests.setUp(self)
        self.messages = []

        def publish(message):
            self.messages.append(message)
            return SimpleNamespace(seq=len(self.messages), sse_id="123:1",
                                   event_id=message["id"])

        for module in (HueObjects, Scene, SmartScene):
            patcher = patch.object(module, "StreamEvent", side_effect=publish)
            patcher.start()
            self.addCleanup(patcher.stop)
        cache = patch.object(v2restapi, "v2Resources",
                             {rtype: {} for rtype in v2restapi.v2Resources})
        cache.start()
        self.addCleanup(cache.stop)
        self.save = patch.object(v2restapi.configManager.bridgeConfig, "save_config").start()
        self.addCleanup(patch.stopall)
        patch.object(v2restapi.configManager.bridgeConfig, "mark_dirty").start()
        CONFIG.update({"lights": {}, "groups": {}, "scenes": {}, "smart_scene": {}})

    def create(self, rtype, rid=RID, key="7"):
        data = {"name": "PRIVATE_SCENE_CANARY", "id_v1": key, "id_v2": rid,
                "owner": SimpleNamespace(username="test")}
        cls = Scene.Scene if rtype == "scene" else SmartScene.SmartScene
        target = cls(data)
        CONFIG["scenes" if rtype == "scene" else "smart_scene"][key] = target
        return target

    def delete(self, rtype, rid):
        with self.app.test_request_context(method="DELETE",
                                          headers={"hue-application-key": "test"}):
            return self.app.make_response(v2restapi.ClipV2ResourceId().delete(rtype, rid))

    def deletes(self, rid):
        return [event for event in self.messages
                if event["type"] == "delete" and event["data"][0]["id"] == rid]

    def test_retained_scene_delete_is_immediate_and_finalizer_is_not_extra(self):
        self.check_retained("scene", "scenes")

    def test_retained_smart_scene_delete_is_immediate_and_finalizer_is_not_extra(self):
        self.check_retained("smart_scene", "smart_scene")

    def check_retained(self, rtype, store):
        target = self.create(rtype)
        sibling = self.create(rtype, SIBLING, "12")
        self.save.reset_mock()
        response = self.delete(rtype, RID)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"data": [{"rid": RID, "rtype": rtype}]})
        self.assertEqual(CONFIG[store], {"12": sibling})
        self.assertEqual(len(self.deletes(RID)), 1)
        event = self.deletes(RID)[-1]
        self.assertEqual(event["data"], [{"id": RID, "type": rtype}])
        self.assertEqual(event["id_v1"], "/" + store + "/7")
        self.save.assert_called_once_with(backup=False, resource=store)
        ref = weakref.ref(target)
        del target
        gc.collect()
        self.assertIsNone(ref())
        self.assertEqual(len(self.deletes(RID)), 1)

    def test_repeat_missing_and_stale_cache_are_404_without_publication(self):
        for rtype, store in (("scene", "scenes"), ("smart_scene", "smart_scene")):
            with self.subTest(rtype=rtype):
                target = self.create(rtype)
                v2restapi.v2Resources[rtype][RID] = weakref.ref(target)
                self.assertEqual(self.delete(rtype, RID).status_code, 200)
                self.messages.clear()
                for rid in (RID, SIBLING):
                    response = self.delete(rtype, rid)
                    self.assertEqual(response.status_code, 404)
                    self.assertEqual(response.get_json(), {
                        "errors": [{"description": "Not Found"}], "data": []})
                v2restapi.v2Resources[rtype][RID] = weakref.ref(target)
                self.assertEqual(self.delete(rtype, RID).status_code, 404)
                self.assertNotIn(RID, v2restapi.v2Resources[rtype])
                self.assertEqual(self.messages, [])

    def test_concurrent_repeats_return_200_and_404(self):
        for rtype in ("scene", "smart_scene"):
            with self.subTest(rtype=rtype):
                target = self.create(rtype)
                barrier = threading.Barrier(3)
                results, failures = [], []

                def run():
                    try:
                        barrier.wait(timeout=5)
                        results.append(self.delete(rtype, RID).status_code)
                    except Exception as error:
                        failures.append(error)

                threads = [threading.Thread(target=run) for _ in range(2)]
                for thread in threads:
                    thread.start()
                barrier.wait(timeout=5)
                for thread in threads:
                    thread.join(timeout=5)
                    self.assertFalse(thread.is_alive())
                self.assertEqual(failures, [])
                self.assertEqual(sorted(results), [200, 404])
                self.assertEqual(len(self.deletes(RID)), 1)
                self.messages.clear()

    def test_publication_precedes_save_and_save_failure_is_honest(self):
        target = self.create("smart_scene")

        def fail(**kwargs):
            self.assertEqual(len(self.deletes(RID)), 1)
            raise OSError("PRIVATE_SAVE_FAILURE_CANARY")

        self.save.side_effect = fail
        with self.assertLogs(v2restapi.logging, level="INFO") as logs:
            with self.assertRaises(OSError):
                self.delete("smart_scene", RID)
        text = "\n".join(logs.output)
        self.assertIn("save_call=failed", text)
        self.assertNotIn("PRIVATE_SAVE_FAILURE_CANARY", text)
        self.assertNotIn("persisted", text)
        self.assertEqual(CONFIG["smart_scene"], {})

    def test_finalizer_fallback_and_partial_construction(self):
        for rtype, cls, store in (("scene", Scene.Scene, "scenes"),
                                  ("smart_scene", SmartScene.SmartScene, "smart_scene")):
            gc.collect()
            self.messages.clear()
            target = self.create(rtype)
            del CONFIG[store]["7"]
            del target
            gc.collect()
            self.assertEqual(len(self.deletes(RID)), 1)
            self.messages.clear()
            partial = cls.__new__(cls)
            partial.__del__()

    def test_delete_logs_correlate_without_names_or_arbitrary_path_text(self):
        target = self.create("smart_scene")
        with self.assertLogs(v2restapi.logging, level="INFO") as route_logs, \
                self.assertLogs(HueObjects.logging, level="INFO") as event_logs:
            self.delete("smart_scene", RID)
            self.delete("smart_scene", "PRIVATE_PATH_CANARY\ninjected=true")
        text = "\n".join(route_logs.output + event_logs.output)
        self.assertIn(RID, text)
        self.assertIn("source=v2", text)
        self.assertIn("save_call=returned", text)
        self.assertNotIn("PRIVATE_SCENE_CANARY", text)
        self.assertNotIn("PRIVATE_PATH_CANARY", text)
        self.assertNotIn("test-key", text)
