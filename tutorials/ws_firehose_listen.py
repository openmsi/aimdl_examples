"""Raw Girder WebSocket firehose listener — a diagnostic, not part of the app.

Prints EVERY frame that arrives on wss://girder.htmdec.org/notifications/public,
bypassing both the `type == pdv_alpss_result` filter and the `itemId` dedup that the
live dashboard applies. Run this in one terminal while running make_notifications.ipynb
in another: each metadata write there should show up here within a second or two.

    python tutorials/ws_firehose_listen.py

What you're checking:
  * Does ANYTHING arrive?           -> is the socket/auth working at all
  * type == "pdv_alpss_result"?     -> does the server hook emit the fat notification
  * itemId / experiment_date / metric fields present in data? -> is the payload complete

Ctrl-C to stop.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import websocket  # websocket-client
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from aimdl_examples import get_girder_client
from aimdl_examples.live import GIRDER_WS_URL

DATA_TYPE = "pdv_alpss_result"

gc = get_girder_client()
print(f"authenticated to {gc.urlBase}  (token {'present' if getattr(gc, 'token', None) else 'MISSING'})")
print(f"listening on {GIRDER_WS_URL}\n  Ctrl-C to stop\n")

_count = {"all": 0, "ours": 0}


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def on_open(ws):
    # Same handshake GirderWebSocketSource._on_open uses: reuse the client's token.
    ws.send(json.dumps({"type": "auth", "token": getattr(gc, "token", None)}))
    print(f"[{_stamp()}] socket open, auth sent — waiting for frames...")


def on_message(ws, raw):
    _count["all"] += 1
    try:
        msg = json.loads(raw)
    except Exception:
        print(f"[{_stamp()}] #{_count['all']} non-JSON frame: {raw[:200]!r}")
        return

    mtype = msg.get("type")
    if mtype == DATA_TYPE:
        _count["ours"] += 1
        data = msg.get("data", {}) or {}
        print(f"[{_stamp()}] #{_count['all']}  *** {mtype} ***  (ours #{_count['ours']})")
        print(f"    itemId          = {data.get('itemId')}")
        print(f"    igsn            = {data.get('igsn')}")
        print(f"    experiment_date = {data.get('experiment_date')}")
        print(f"    Velocity at Max Compression = {data.get('Velocity at Max Compression')}")
        print(f"    Spall Strength              = {data.get('Spall Strength')}")
        keys = sorted(data.keys())
        print(f"    all data keys   = {keys}")
    else:
        # firehose noise: other item types, jobs, etc. Show type so you can see traffic.
        print(f"[{_stamp()}] #{_count['all']}  type={mtype!r} (ignored by dashboard)")


def on_error(ws, err):
    print(f"[{_stamp()}] ERROR: {err}")


def on_close(ws, code, reason):
    print(f"[{_stamp()}] closed ({code} {reason}). "
          f"total frames={_count['all']}, pdv_alpss_result={_count['ours']}")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        GIRDER_WS_URL, on_open=on_open, on_message=on_message,
        on_error=on_error, on_close=on_close,
    )
    try:
        ws.run_forever()
    except KeyboardInterrupt:
        ws.close()
        print(f"\nstopped. total frames={_count['all']}, pdv_alpss_result={_count['ours']}")
