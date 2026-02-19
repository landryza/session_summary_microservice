
"""
Session Summary Microservice (CS361-friendly, Swagger-auth works)

Features:
- Record session events (bet | win | loss)
- End session -> returns computed summary
- Get most recent session summary
- Export a specific session summary as JSON (bare object)
- /ping echo endpoint for CS361 demo

Run:
  pip install -r requirements.txt
  uvicorn main:app --host 127.0.0.1 --port 5003 --reload
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

DATA_FILE = "summaries.json"  # finished summaries persisted here (per user)

# ----------------------------
# Helpers: time & summary math
# ----------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def parse_iso(ts: str) -> datetime:
    # Accept trailing Z or timezone offset
    if ts.endswith('Z'):
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    return datetime.fromisoformat(ts)

def _int_if_whole(x: float):
    try:
        return int(x) if float(x).is_integer() else float(x)
    except Exception:
        return x

# ----------------------------
# Pydantic models
# ----------------------------

class EventIn(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=128)
    event_type: str = Field(..., pattern=r"^(bet|win|loss)$")
    amount: float = Field(..., ge=0)
    timestamp: Optional[str] = Field(None, description="ISO-8601 timestamp; defaults to now if omitted")

class EndSessionIn(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=128)

class Summary(BaseModel):
    session_id: str
    user_id: str
    start_time: str
    end_time: str
    rounds: int
    total_bets: float | int
    total_wins: float | int
    net_change: float | int

class WrappedSummary(BaseModel):
    ok: bool
    summary: Summary

class LatestResponse(BaseModel):
    ok: bool
    summary: Optional[Summary] = None
    message: Optional[str] = None

class PingRequest(BaseModel):
    message: str

class PingResponse(BaseModel):
    message: str

# ----------------------------
# In-memory stores
# ----------------------------

# TOKEN_USER maps bearer token -> user_id (first seen via X-User-Id on request)
TOKEN_USER: Dict[str, str] = {}

# ACTIVE[user_id][session_id] = { 'start_time': str|None, 'end_time': str|None, 'events': [ {event} ] }
ACTIVE: Dict[str, Dict[str, Dict[str, object]]] = {}

# FINISHED[user_id] = [Summary dicts]
FINISHED: Dict[str, List[Dict[str, object]]] = {}

# ----------------------------
# Persistence for finished summaries (simple file)
# ----------------------------

def load_finished() -> None:
    global FINISHED
    if not os.path.exists(DATA_FILE):
        FINISHED = {}
        return
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # validate minimal structure
        FINISHED = data if isinstance(data, dict) else {}
    except Exception:
        FINISHED = {}

def save_finished() -> None:
    tmp = DATA_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(FINISHED, f, indent=2)
    os.replace(tmp, DATA_FILE)

# ----------------------------
# FastAPI app + security
# ----------------------------

app = FastAPI(title="Session Summary Microservice", version="1.0.0")
security = HTTPBearer(auto_error=True)

@app.on_event("startup")
def _startup() -> None:
    load_finished()

# Dependency: get user_id from bearer token, binding on first use via X-User-Id
def current_user_id(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
) -> str:
    token = credentials.credentials  # raw token value (no "Bearer ")
    user_id = TOKEN_USER.get(token)
    if user_id:
        # If client also sent X-User-Id, enforce consistency
        if x_user_id and x_user_id != user_id:
            raise HTTPException(status_code=403, detail="Token already bound to a different user")
        return user_id
    # First time seeing this token; require X-User-Id to bind
    if not x_user_id:
        raise HTTPException(status_code=401, detail="Unknown token. Include X-User-Id to bind.")
    TOKEN_USER[token] = x_user_id
    return x_user_id

# ----------------------------
# Core logic
# ----------------------------

def compute_summary(user_id: str, session_id: str) -> Summary:
    session = ACTIVE.get(user_id, {}).get(session_id)
    if not session:
        # Try finished summaries
        for s in FINISHED.get(user_id, []):
            if s.get('session_id') == session_id:
                return Summary(**s)  # type: ignore[arg-type]
        raise HTTPException(status_code=404, detail="Session not found")

    events = session.get('events', [])
    # Count one round per bet
    rounds = sum(1 for e in events if e.get('event_type') == 'bet')
    total_bets = sum(float(e.get('amount', 0)) for e in events if e.get('event_type') == 'bet')
    total_wins = sum(float(e.get('amount', 0)) for e in events if e.get('event_type') == 'win')
    net_change = total_wins - total_bets

    # Timestamps
    ts_list: List[datetime] = []
    for e in events:
        ts = e.get('timestamp')
        if isinstance(ts, str):
            try:
                ts_list.append(parse_iso(ts))
            except Exception:
                pass
    start_time = session.get('start_time')
    end_time = session.get('end_time')
    if ts_list:
        ts_list.sort()
        start_time = ts_list[0].replace(microsecond=0, tzinfo=timezone.utc).isoformat()
        if end_time is None:
            end_time = ts_list[-1].replace(microsecond=0, tzinfo=timezone.utc).isoformat()

    result = Summary(
        session_id=session_id,
        user_id=user_id,
        start_time=start_time or now_iso(),
        end_time=end_time or now_iso(),
        rounds=int(rounds),
        total_bets=_int_if_whole(total_bets),
        total_wins=_int_if_whole(total_wins),
        net_change=_int_if_whole(net_change),
    )
    return result

# ----------------------------
# Endpoints
# ----------------------------

@app.get("/")
def root():
    return {"ok": True, "service": "session_summary_microservice", "docs": "/docs"}

@app.post("/session/event")
def record_event(req: EventIn, user_id: str = Depends(current_user_id)):
    # Validate event_type via regex already, validate timestamp format if provided
    ts = req.timestamp
    if ts is None:
        ts = now_iso()
    else:
        try:
            _ = parse_iso(ts)
        except Exception:
            raise HTTPException(status_code=400, detail="timestamp must be ISO-8601 (e.g., 2026-02-12T14:05:00Z)")

    user_sessions = ACTIVE.setdefault(user_id, {})
    sess = user_sessions.get(req.session_id)
    if sess is None:
        sess = {"start_time": ts, "end_time": None, "events": []}
        user_sessions[req.session_id] = sess

    sess["events"].append({
        "event_type": req.event_type,
        "amount": float(req.amount),
        "timestamp": ts,
    })

    return {"ok": True, "message": "Event recorded"}

@app.post("/session/end", response_model=WrappedSummary)
def end_session(req: EndSessionIn, user_id: str = Depends(current_user_id)) -> WrappedSummary:
    # Mark end time
    sess = ACTIVE.get(user_id, {}).get(req.session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Session not found")
    if sess.get("end_time") is None:
        sess["end_time"] = now_iso()

    summary = compute_summary(user_id, req.session_id).model_dump()

    # Save into FINISHED (replace if exists)
    items = FINISHED.setdefault(user_id, [])
    for i, s in enumerate(items):
        if s.get("session_id") == req.session_id:
            items[i] = summary
            break
    else:
        items.append(summary)
    save_finished()

    return WrappedSummary(ok=True, summary=Summary(**summary))

@app.get("/session/latest", response_model=LatestResponse)
def get_latest(user_id: str = Depends(current_user_id)) -> LatestResponse:
    items = FINISHED.get(user_id, [])
    if not items:
        return LatestResponse(ok=False, message="No session summary found for this user.")
    latest = max(items, key=lambda s: s.get("end_time") or "")
    return LatestResponse(ok=True, summary=Summary(**latest))

@app.get("/session/export")
def export_session(
    session_id: Optional[str] = Query(default=None, alias="session_id"),
    legacy_sess_id: Optional[str] = Query(default=None, alias="sess_id"),
    user_id: str = Depends(current_user_id),
):
    sid = session_id or legacy_sess_id
    if not sid:
        raise HTTPException(status_code=400, detail="session_id query param is required")

    # Prefer finished summary, else compute from active
    for s in FINISHED.get(user_id, []):
        if s.get("session_id") == sid:
            return JSONResponse(content=s)
    # Try active
    try:
        summary = compute_summary(user_id, sid).model_dump()
    except HTTPException as e:
        if e.status_code == 404:
            raise
        raise
    return JSONResponse(content=summary)

@app.post("/ping", response_model=PingResponse)
def ping(req: PingRequest) -> PingResponse:
    return PingResponse(message=req.message)

# If executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5003, reload=True)
