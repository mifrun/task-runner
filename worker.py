# worker.py
# Notion Task Runner: берет задачи из БД Notion и выполняет разрешенные действия.
# env: NOTION_TOKEN, NOTION_DATABASE_ID

import os
import json
import shlex
import subprocess
import time
from typing import Callable

from notion_client import Client
from dotenv import load_dotenv

# ------------ Инициализация ------------

load_dotenv()
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]
notion = Client(auth=NOTION_TOKEN)

# Белые списки
ALLOWED_ACTIONS = {"run_script", "call_api", "codex_apply"}
ALLOWED_SCRIPTS = {"build.sh", "sync_data.sh"}          # ./tasks/*
ALLOWED_URLS = {"https://httpbin.org/post"}            # тестовый URL

# ------------ Вспомогательные утилиты ------------

def _retry(n: int = 5, delay: float = 0.8):
    def deco(fn: Callable):
        def wrap(*a, **kw):
            last = None
            for i in range(1, n + 1):
                try:
                    return fn(*a, **kw)
                except Exception as e:
                    last = e
                    print(f"[RETRY] {fn.__name__} failed ({i}/{n}): {e}")
                    time.sleep(delay * i)
            raise last
        return wrap
    return deco

@_retry()
def notion_update_page(page_id, properties):
    return notion.pages.update(page_id=page_id, properties=properties)

@_retry()
def notion_append_block(block_id, children):
    return notion.blocks.children.append(block_id=block_id, children=children)

@_retry()
def notion_retrieve_page(page_id):
    return notion.pages.retrieve(page_id=page_id)

def debug_dump_db_schema():
    try:
        db = notion.databases.retrieve(DB_ID)
        print("=== DB PROPERTIES ===")
        for name, meta in db.get("properties", {}).items():
            print(f"- {name}: {meta.get('type')}")
        print("=====================")
    except Exception as e:
        print(f"[WARN] debug_dump_db_schema failed: {e}")

def _prop(props: dict, name: str, default=None):
    return props.get(name) or default

# ------------ Запись статуса и логов ------------

def set_status(page_id: str, status: str, logs: str | None = None):
    print(f"set_status[{status}] → updating properties...")
    props = {"Status": {"select": {"name": status}}}

    snippet = None
    if logs:
        snippet = str(logs)[:1800]
        props["Logs"] = {"rich_text": [{"type": "text", "text": {"content": snippet}}]}
        props["LogsPlain"] = {"rich_text": [{"type": "text", "text": {"content": snippet}}]}

    try:
        notion_update_page(page_id, props)
        print(f"set_status[{status}] → properties updated")
    except Exception as e:
        print(f"[ERR] pages.update failed: {e}")

    if snippet:
        try:
            notion_append_block(page_id, [{
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": snippet}}]}
            }])
            print("set_status → block appended")
        except Exception as e:
            print(f"[WARN] blocks.append failed: {e}")

# ------------ Логика очереди: приоритет, зависимости, попытки ------------

def fetch_ready_tasks():
    print("Querying Ready tasks…")
    # Фильтр Ready + сортировка по Priority (asc) и последнему редактированию
    res = notion.databases.query(
        database_id=DB_ID,
        filter={"property": "Status", "select": {"equals": "Ready"}},
        sorts=[
            {"property": "Priority", "direction": "ascending"},
            {"timestamp": "last_edited_time", "direction": "ascending"},
        ],
        page_size=50,
    )
    results = res.get("results", [])
    print(f"Found {len(results)} ready task(s).")
    return results

def can_start(page: dict) -> bool:
    """Проверяем, что все зависимости (DependsOn) находятся в статусе Done."""
    props = page["properties"]
    rel = _prop(props, "DependsOn", {}).get("relation", [])
    if not rel:
        return True
    for r in rel:
        dep_id = r["id"]
        try:
            dep = notion_retrieve_page(dep_id)
        except Exception as e:
            print(f"[WARN] retrieve dep failed ({dep_id}): {e}")
            return False
        st = _prop(dep["properties"], "Status", {}).get("select", {})
        if (st.get("name") or "") != "Done":
            return False
    return True

def inc_attempt(page_id: str, current: int):
    try:
        notion_update_page(page_id, {"Attempts": {"number": (current + 1)}})
    except Exception as e:
        print(f"[WARN] Attempts++ failed: {e}")

# ------------ Действия ------------

def safe_run(cmd: str | None):
    parts = shlex.split(cmd or "")
    if len(parts) != 1:
        raise RuntimeError("Only single command name allowed in Payload.cmd")
    name = os.path.basename(parts[0])
    if name not in ALLOWED_SCRIPTS:
        raise RuntimeError(f"Script '{name}' not allowed (not in ALLOWED_SCRIPTS)")
    print(f"safe_run: {name}")
    proc = subprocess.run(["/bin/bash", f"./tasks/{name}"], capture_output=True, text=True, timeout=300)
    out = (proc.stdout or "") + (proc.stderr or "")
    print(f"safe_run exit code: {proc.returncode}")
    return proc.returncode, out

def call_api(payload: dict):
    import requests
    url = payload.get("url")
    method = (payload.get("method") or "GET").upper()
    body = payload.get("body")
    if not url or url not in ALLOWED_URLS:
        raise RuntimeError("URL not allowed (not in ALLOWED_URLS)")
    print(f"call_api: {url} {method}")
    r = requests.request(method, url, json=body, timeout=20)
    text = f"{r.status_code} {r.text[:1500]}"
    return r.status_code, text

def codex_apply(payload: dict):
    """
    Выполнение кодовой задачи через Codex CLI.
    Payload:
    {
      "spec": "что нужно изменить/реализовать",
      "repo_path": ".",        # путь к репо (в Actions это корень)
      "timeout_sec": 1800
    }
    """
    spec = (payload.get("spec") or "").strip()
    if not spec:
        raise RuntimeError("Missing spec for codex_apply")
    repo = payload.get("repo_path", ".")
    to = int(payload.get("timeout_sec", 1800))
    print("codex_apply: starting Codex CLI…")
    proc = subprocess.run(
        ["codex", "apply", "--repo", repo, "--spec", spec, "--yes"],
        capture_output=True, text=True, timeout=to
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    print(f"codex_apply exit code: {proc.returncode}")
    return proc.returncode, out

# ------------ Обработка одной задачи ------------

def handle_task(page: dict):
    page_id = page["id"]
    title = page["properties"]["Name"]["title"][0]["plain_text"] if page["properties"]["Name"]["title"] else "(no title)"
    props = page["properties"]
    action = props["Action"]["select"]["name"] if props.get("Action") and props["Action"]["select"] else None
    payload_txt = "".join([t["plain_text"] for t in props.get("Payload", {}).get("rich_text", [])]) or "{}"

    # попытки
    attempts = _prop(props, "Attempts", {}).get("number", 0) or 0
    max_attempts = _prop(props, "MaxAttempts", {}).get("number", 3) or 3
    if attempts >= max_attempts:
        print(f"Skip (max attempts reached): {title} [{page_id}]")
        return
    inc_attempt(page_id, attempts)

    print(f"Handling: {title} [{page_id}] action={action} payload={payload_txt}")

    try:
        payload = json.loads(payload_txt)
    except Exception:
        payload = {}

    # Running
    print(f"TRY set Running for {page_id}")
    set_status(page_id, "Running")
    print("Running set OK")

    try:
        if action == "run_script":
            code, out = safe_run(payload.get("cmd"))
        elif action == "call_api":
            code, out = call_api(payload)
        elif action == "codex_apply":
            code, out = codex_apply(payload)
        else:
            raise RuntimeError(f"Unknown or not allowed action: {action}")

        # Успех — код 0 или HTTP 2xx
        if (isinstance(code, int) and code == 0) or (isinstance(code, int) and 200 <= code < 300):
            set_status(page_id, "Done", out)
        else:
            set_status(page_id, "Failed", out)
    except Exception as e:
        set_status(page_id, "Failed", f"Error: {e}")

# ------------ Главный цикл ------------

def main():
    print("Starting worker…")
    print(f"DB_ID: {DB_ID}")
    debug_dump_db_schema()

    tasks = fetch_ready_tasks()
    # фильтруем по зависимостям
    runnable = []
    waiting = []
    for t in tasks:
        if can_start(t):
            runnable.append(t)
        else:
            waiting.append(t)

    if waiting:
        print(f"Waiting (deps not done): {len(waiting)} task(s).")

    for i, t in enumerate(runnable, 1):
        print(f"Task {i}/{len(runnable)}")
        handle_task(t)
        time.sleep(1.0)  # легкая пауза от rate limits

    print("Worker finished.")

if __name__ == "__main__":
    main()
