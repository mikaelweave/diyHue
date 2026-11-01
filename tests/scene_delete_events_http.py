"""Exercise the synthetic server, never a live bridge."""
from collections import Counter
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request

import yaml


ROOT = "http://127.0.0.1:8080"
HEADERS = {"hue-application-key": "scene-delete-event-test-key"}
CONFIG = Path("/tmp/diyhue-scene-events-config")
STATE = Path("/tmp/diyhue-scene-events-state.json")


def request(method, path, data=None):
    headers = dict(HEADERS)
    payload = json.dumps(data).encode() if data is not None else None
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(ROOT + path, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def api(method, resource, data=None, expected=200):
    status, body = request(method, "/clip/v2/resource/" + resource, data)
    assert status == expected, (status, body)
    return body


def open_stream(last_id=None):
    headers = dict(HEADERS)
    if last_id is not None:
        headers["Last-Event-ID"] = last_id
    response = urllib.request.urlopen(
        urllib.request.Request(ROOT + "/eventstream/clip/v2", headers=headers), timeout=5
    )
    assert response.readline() == b": hi\n"
    assert response.readline() == b"\n"
    return response


def frame(response):
    event_id, data = None, None
    while True:
        raw = response.readline()
        if not raw:
            raise EOFError("Synthetic event stream closed before the expected frame")
        line = raw.decode().rstrip("\r\n")
        if not line:
            if data is not None:
                return event_id, data
        elif line.startswith("id: "):
            event_id = line[4:]
        elif line.startswith("data: "):
            data = json.loads(line[6:])[0]


def until(response, rid, kind, counts):
    for _ in range(30):
        event_id, event = frame(response)
        for item in event["data"]:
            if event["type"] == "delete":
                counts[item["id"]] += 1
        if event["type"] == kind and any(item["id"] == rid for item in event["data"]):
            return event_id, event
    raise AssertionError("Expected synthetic event was not found")


def disk_ids(store):
    objects = yaml.safe_load((CONFIG / (store + ".yaml")).read_text())
    return {obj["id_v2"] for obj in objects.values()}


def run():
    api("POST", "room", {"metadata": {"name": "Synthetic room"}, "children": []})
    room = next(item["id"] for item in api("GET", "room")["data"]
                if item["metadata"]["name"] == "Synthetic room")
    group = {"rid": room, "rtype": "room"}
    scenes = []
    for name in ("Synthetic target", "Synthetic survivor"):
        scenes.append(api("POST", "scene", {
            "metadata": {"name": name}, "group": group, "actions": [],
        })["data"][0]["rid"])
    smart = []
    for name in ("Synthetic cycle target", "Synthetic cycle survivor"):
        smart.append(api("POST", "smart_scene", {
            "metadata": {"name": name}, "group": group,
            "recall": {"action": "deactivate"},
            "week_timeslots": [{
                "timeslots": [{
                    "start_time": {"kind": "time", "time": {"hour": 7, "minute": 0, "second": 0}},
                    "target": {"rid": scenes[1], "rtype": "scene"},
                }],
                "recurrence": ["monday"],
            }],
        })["data"][0]["rid"])
    targets = {"scene": scenes[0], "smart_scene": smart[0]}
    survivors = {"scene": scenes[1], "smart_scene": smart[1]}
    assert request("POST", "/testing/persist", {})[0] == 200
    for rtype, rid in targets.items():
        assert request("POST", "/testing/retain", {"type": rtype, "id": rid})[0] == 200
    streams = [open_stream(), open_stream()]
    counts = [Counter(), Counter()]
    replay_id = None
    try:
        for rtype, rid in targets.items():
            api("DELETE", rtype + "/" + rid)
            delivered = [until(stream, rid, "delete", count)
                         for stream, count in zip(streams, counts)]
            assert delivered[0] == delivered[1]
            assert delivered[0][1]["data"] == [{"id": rid, "type": rtype}]
            if rtype == "scene":
                replay_id = delivered[0][0]
            api("DELETE", rtype + "/" + rid, expected=404)
            assert api("GET", rtype + "/" + rid)["data"] == []
            store = "scenes" if rtype == "scene" else "smart_scene"
            assert disk_ids(store) == {survivors[rtype]}
            print(f"{rtype}: HTTP 200; two identical delete frames while retained; retry HTTP 404; disk correct",
                  flush=True)
        assert request("POST", "/testing/release", {})[0] == 200
        marker = api("POST", "scene", {
            "metadata": {"name": "Synthetic marker"}, "group": group, "actions": [],
        })["data"][0]["rid"]
        for stream, count in zip(streams, counts):
            until(stream, marker, "add", count)
            assert count == Counter(targets.values())
        api("DELETE", "scene/" + marker)
        with open_stream(replay_id) as replay:
            event_id, event = frame(replay)
            assert event["type"] == "delete"
            assert event["data"] == [{"id": smart[0], "type": "smart_scene"}]
        print("No duplicate after retry or finalization; HTTP reconnect replays the next deletion", flush=True)
    finally:
        for stream in streams:
            stream.close()
    STATE.write_text(json.dumps({"room": room, "targets": targets, "survivors": survivors}))


def restart(empty=False):
    saved = json.loads(STATE.read_text())
    for rtype, survivor in saved["survivors"].items():
        ids = {item["id"] for item in api("GET", rtype)["data"]}
        assert ids == (set() if empty else {survivor})
        if not empty:
            api("DELETE", rtype + "/" + survivor)
            assert disk_ids("scenes" if rtype == "scene" else "smart_scene") == set()
    assert saved["room"] in {item["id"] for item in api("GET", "room")["data"]}
    print("Restart preserves scene deletion and room; " +
          ("empty collections remain empty" if empty else "final-scene deletions persist"), flush=True)


if __name__ == "__main__":
    for attempt in range(40):
        try:
            if request("GET", "/clip/v2/resource/scene")[0] == 200:
                break
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    else:
        raise RuntimeError("Synthetic server did not become ready")
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    if mode == "run":
        run()
    elif mode in ("restart", "restart-empty"):
        restart(mode == "restart-empty")
    else:
        raise ValueError(mode)
