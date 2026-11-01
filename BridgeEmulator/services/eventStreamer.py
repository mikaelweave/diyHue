import logManager
from flask import Response, stream_with_context, Blueprint, request
from itertools import count
from time import monotonic
import HueObjects

logging = logManager.logger.get_logger(__name__)
stream = Blueprint('stream', __name__)
KEEPALIVE_S = 15.0
MAX_STREAM_S = 300.0
_connection_ids = count(1)


@stream.route('/eventstream/clip/v2')
def streamV2Events():
    journal = HueObjects.events
    resume = journal.resume(request.headers.get("Last-Event-ID"))
    connection = next(_connection_ids)

    def generate():
        cursor = resume.cursor
        reason = "disconnect"
        deadline = monotonic() + MAX_STREAM_S
        gap = resume.start not in ("live", "replay")
        HueObjects.event_log(logging, 30 if gap else 20, "sse open",
                             connection=connection, start=resume.start, cursor=cursor,
                             req_seq=resume.req_seq,
                             oldest=resume.oldest_seq, head=resume.head_seq,
                             missed=resume.missed)
        try:
            yield ": hi\n\n"
            if gap:
                yield ": stream resume gap; unrecoverable via sse; live from head\n\n"
            while True:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                batch = journal.wait_after(cursor, min(KEEPALIVE_S, remaining))
                if batch.missed:
                    reason = "gap"
                    HueObjects.event_log(logging, 30, "sse gap", connection=connection,
                                         cursor=cursor, oldest=batch.oldest_seq,
                                         head=batch.head_seq, missed=batch.missed)
                    yield ": stream gap; unrecoverable via sse; client rest resync required\n\n"
                    return
                if not batch.entries:
                    yield ": keepalive\n\n"
                    continue
                for entry in batch.entries:
                    if monotonic() >= deadline:
                        reason = "lifetime"
                        return
                    cursor = entry.seq
                    HueObjects.event_log(logging, 20 if entry.kind in ("delete", "add") else 10,
                                         "sse yield_to_wsgi", connection=connection,
                                         seq=entry.seq, sse_id=entry.sse_id,
                                         event_id=entry.event_id, type=entry.kind,
                                         resource=",".join(entry.refs),
                                         status="yielded", note="attempted_not_received")
                    yield f"id: {entry.sse_id}\ndata: {entry.data}\n\n"
            reason = "lifetime"
        finally:
            HueObjects.event_log(logging, 20, "sse close", connection=connection,
                                 reason=reason, cursor=cursor)

    return Response(stream_with_context(generate()), mimetype='text/event-stream; charset=utf-8')
