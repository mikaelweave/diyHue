import uuid
import logManager
import random
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
from threading import Condition, Lock, RLock
from time import time_ns

logging = logManager.logger.get_logger(__name__)


def log_uuid(value):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError):
        return "invalid"


def event_log(logger, level, action, **fields):
    logger.log(level, "%s %s", action,
               " ".join(f"{key}={value}" for key, value in fields.items()), stacklevel=2)


@dataclass(frozen=True)
class StreamEntry:
    seq: int
    sse_id: str
    data: str
    byte_len: int
    kind: str
    refs: tuple
    event_id: str

    @property
    def message(self):
        return json.loads(self.data)[0]


@dataclass(frozen=True)
class Resume:
    cursor: int
    start: str
    req_seq: int | None
    oldest_seq: int
    head_seq: int
    missed: int = 0


@dataclass(frozen=True)
class Batch:
    entries: tuple
    missed: int
    oldest_seq: int
    head_seq: int


class EventJournal:
    def __init__(self, *, max_events=1024, max_bytes=4 * 1024 * 1024):
        self.epoch = time_ns()
        self.max_events = max_events
        self.max_bytes = max_bytes
        self._entries = deque()
        self._total_bytes = 0
        self._last_seq = 0
        self._changed = Condition(RLock())

    def head(self):
        with self._changed:
            return self._last_seq

    def publish(self, message):
        data = json.dumps([message], separators=(",", ":"))
        byte_len = len(data.encode("utf-8"))
        kind = message.get("type")
        kind = kind if kind in ("add", "delete", "update") else "unknown"
        resource_types = {
            "scene", "smart_scene", "light", "grouped_light", "room", "zone",
            "device", "entertainment_configuration", "entertainment",
            "behavior_instance", "geofence_client", "zigbee_connectivity",
            "motion", "light_level", "temperature", "button", "device_power",
            "relative_rotary", "zigbee_device_discovery",
        }
        refs = tuple(
            f"{item.get('type') if item.get('type') in resource_types else 'unknown'}/{log_uuid(item.get('id'))}"
            for item in message.get("data", [])
        )
        event_id = log_uuid(message.get("id"))
        with self._changed:
            while True:
                head = self._last_seq
                entry = StreamEntry(head + 1, f"{self.epoch}:{head + 1}",
                                    data, byte_len, kind, refs, event_id)
                # Allocation may run a finalizer that publishes under this RLock.
                if self._last_seq == head:
                    break
            self._entries.append(entry)
            self._last_seq = entry.seq
            self._total_bytes += byte_len
            while len(self._entries) > 1 and (
                    len(self._entries) > self.max_events or self._total_bytes > self.max_bytes):
                self._total_bytes -= self._entries.popleft().byte_len
            self._changed.notify_all()
        event_log(logging, 20 if kind in ("delete", "add") else 10, "event published",
                  seq=entry.seq, sse_id=entry.sse_id, event_id=event_id,
                  type=kind, resource=",".join(refs), status="published")
        if byte_len > self.max_bytes:
            event_log(logging, 30, "event oversized", seq=entry.seq,
                      sse_id=entry.sse_id, type=kind, resource=",".join(refs),
                      bytes=byte_len, max_bytes=self.max_bytes)
        return entry

    def resume(self, last_event_id):
        req_seq = None
        epoch = None
        if isinstance(last_event_id, str) and len(last_event_id) <= 128:
            match = re.fullmatch(r"([0-9]+):([0-9]+)", last_event_id)
            if match:
                epoch, req_seq = map(int, match.groups())
        with self._changed:
            head = self._last_seq
            oldest = self._entries[0].seq if self._entries else head + 1
        missed = 0
        cursor = head
        if last_event_id is None:
            start = "live"
        elif epoch is None:
            start = "invalid_id"
        elif epoch != self.epoch:
            start = "gap_epoch"
        elif req_seq > head:
            start = "invalid_id"
        elif req_seq < oldest - 1:
            start = "gap_evicted"
            missed = oldest - 1 - req_seq
        else:
            start = "replay"
            cursor = req_seq
        return Resume(cursor, start, req_seq, oldest, head, missed)

    def wait_after(self, cursor, timeout):
        with self._changed:
            self._changed.wait_for(lambda: self._last_seq > cursor, timeout)
            snapshot = tuple(self._entries)
            head = self._last_seq
        oldest = snapshot[0].seq if snapshot else head + 1
        missed = max(0, oldest - 1 - cursor)
        entries = () if missed else tuple(entry for entry in snapshot if entry.seq > cursor)
        return Batch(entries, missed, oldest, head)


events = EventJournal()


def StreamEvent(message):
    return events.publish(message)


class AnnouncesDeletion:
    def _init_deletion_lifecycle(self):
        self._delete_announced = False
        self._delete_lock = Lock()

    def announce_deleted(self, source="v2"):
        lock = getattr(self, "_delete_lock", None)
        if lock is None:
            return None
        with lock:
            if self._delete_announced:
                return None
            message = {
                "creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "data": [{"id": self.id_v2, "type": self.V2_TYPE}],
                "id": str(uuid.uuid4()),
                "type": "delete",
                "id_v1": self.V1_PREFIX + self.id_v1,
            }
            entry = StreamEvent(message)
            self._delete_announced = True
        event_log(logging, 20, "scene delete_published", resource=self.V2_TYPE,
                  rid=log_uuid(self.id_v2), seq=entry.seq, sse_id=entry.sse_id,
                  event_id=entry.event_id, source=source, status="published")
        return entry

    def __del__(self):
        self.announce_deleted(source="finalizer")

def v1StateToV2(v1State):
    v2State = {}
    if "on" in v1State:
        v2State["on"] = {"on": v1State["on"]}
    if "bri" in v1State:
        v2State["dimming"] = {"brightness": round(v1State["bri"] / 2.54, 2)}
    if "ct" in v1State:
        v2State["color_temperature"] = {"mirek": v1State["ct"], "color_temperature_delta": {}}
    if "xy" in v1State:
        v2State["color"] = {
            "xy": {"x": v1State["xy"][0], "y": v1State["xy"][1]}}
    return v2State


def v2StateToV1(v2State):
    v1State = {}
    if "dimming" in v2State:
        v1State["bri"] = int(v2State["dimming"]["brightness"] * 2.54)
    if "on" in v2State:
        v1State["on"] = v2State["on"]["on"]
    if "color_temperature" in v2State:
        v1State["ct"] = v2State["color_temperature"]["mirek"]
    if "color" in v2State:
        if "xy" in v2State["color"]:
            v1State["xy"] = [v2State["color"]["xy"]
                             ["x"], v2State["color"]["xy"]["y"]]
    if "gradient" in v2State:
        v1State["gradient"] = v2State["gradient"]
    if "dynamics" in v2State and "duration" in v2State["dynamics"]:
        # v2 gives the transition in milliseconds, v1 in steps of 100 ms.
        v1State["transitiontime"] = round(v2State["dynamics"]["duration"] / 100)
    if "transitiontime" in v2State:  # to be replaced once api will be public
        v1State["transitiontime"] = v2State["transitiontime"]
    return v1State

def genV2Uuid():
    return str(uuid.uuid4())

def generate_unique_id():
    rand_bytes = [random.randrange(0, 256) for _ in range(3)]
    return "00:17:88:01:00:%02x:%02x:%02x-0b" % (rand_bytes[0], rand_bytes[1], rand_bytes[2])


def setGroupAction(group, state, scene=None):
    lightsState = {}
    if scene != None:
        sceneStates = list(scene.lightstates.items())
        for light, state in sceneStates:
            lightsState[light.id_v1] = state
            if "on" in state and state["on"] == True:
                group.state["any_on"] = True

    else:
        state = incProcess(group.action, state)
        for light in group.lights:
            if light():
                lightsState[light().id_v1] = state
        if "xy" in state:
            group.action["colormode"] = "xy"
        elif "ct" in state:
            group.action["colormode"] = "ct"
        elif "hue" in state or "sat" in state:
            group.action["colormode"] = "hs"

        if "on" in state:
            group.state["any_on"] = state["on"]
            group.state["all_on"] = state["on"]
        group.action.update(state)

    queueState = {}
    for light in group.lights:
        if light() and light().id_v1 in lightsState:  # apply only if the light belong to this group
            for key, value in lightsState[light().id_v1].items():
                if key in light().state:
                    light().state[key] = value
            light().updateLightState(lightsState[light().id_v1])
            # apply max and min brightness limis
            if "bri" in lightsState[light().id_v1]:
                if "min_bri" in light().protocol_cfg and light().protocol_cfg["min_bri"] > lightsState[light().id_v1]["bri"]:
                    lightsState[light().id_v1]["bri"] = light().protocol_cfg["min_bri"]
                if "max_bri" in light().protocol_cfg and light().protocol_cfg["max_bri"] < lightsState[light().id_v1]["bri"]:
                    lightsState[light().id_v1]["bri"] = light().protocol_cfg["max_bri"]
                if  light().protocol == "mqtt" and not light().state["on"]:
                    continue
            # end limits
            if light().protocol in ["native_multi", "mqtt"]:
                if light().protocol_cfg["ip"] not in queueState:
                    queueState[light().protocol_cfg["ip"]] = {
                        "object": light(), "lights": {}}
                if light().protocol == "native_multi":
                    queueState[light().protocol_cfg["ip"]]["lights"][light(
                    ).protocol_cfg["light_nr"]] = lightsState[light().id_v1]
                elif light().protocol == "mqtt":
                    queueState[light().protocol_cfg["ip"]]["lights"][light(
                    ).protocol_cfg["command_topic"]] = lightsState[light().id_v1]
            else:
                light().setV1State(lightsState[light().id_v1])
    for device, state in queueState.items():
        state["object"].setV1State(state)

    group.state = group.update_state()


def incProcess(state, data):
    if "bri_inc" in data:
        state["bri"] += data["bri_inc"]
        if state["bri"] > 254:
            state["bri"] = 254
        elif state["bri"] < 1:
            state["bri"] = 1
        del data["bri_inc"]
        data["bri"] = state["bri"]
    elif "ct_inc" in data:
        state["ct"] += data["ct_inc"]
        if state["ct"] > 500:
            state["ct"] = 500
        elif state["ct"] < 153:
            state["ct"] = 153
        del data["ct_inc"]
        data["ct"] = state["ct"]
    elif "hue_inc" in data:
        state["hue"] += data["hue_inc"]
        if state["hue"] > 65535:
            state["hue"] -= 65535
        elif state["hue"] < 0:
            state["hue"] += 65535
        del data["hue_inc"]
        data["hue"] = state["hue"]
    elif "sat_inc" in data:
        state["sat"] += data["sat_inc"]
        if state["sat"] > 254:
            state["sat"] = 254
        elif state["sat"] < 1:
            state["sat"] = 1
        del data["sat_inc"]
        data["sat"] = state["sat"]

    return data
