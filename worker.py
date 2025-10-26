# worker.py — Notion Task Runner + Epic → Tasks (LLM)
# Требуемые переменные окружения в GitHub Actions:
#   NOTION_TOKEN, NOTION_DATABASE_ID, OPENAI_API_KEY
# Требуемые пакеты в requirements.txt:
#   notion-client==2.2.1, python-dotenv==1.0.1, requests==2.32.3, openai==1.45.0 (SDK не обязателен)

import os, json, shlex, subprocess, time, re
from typing import Callable, List, Dict, Any

from notion_client import Client
from dotenv import load_dotenv
import requests

# ---------- init ----------
load_dotenv()

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]

notion = Client(auth=NOTION_TOKEN)

# Белые списки
ALLOWED_ACTIONS = {"run_script", "call_api", "codex_apply"}   # codex_apply сейчас отключен внутри
ALLOWED_SCRIPTS = {"build.sh", "sync_data.sh"}                # скрипты в ./tasks
ALLOWED_URLS = {"https://httpbin.org/post"}                   # разрешённые call_api URL

# ---------- utils ----------
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

# ---------- status/logs ----------
def set_status(page_id: str, status: str, logs: str | None = None):
    print(f"set_status[{status}] …")
    props = {"Status": {"select": {"name": status}}}
    snippet = (str(logs)[:1800] if logs else None)
    if snippet:
        props["Logs"] = {"rich_text": [{"type": "text", "text": {"content": snippet}}]}
        props["LogsPlain"] = {"rich_text": [{"type": "text", "text": {"content": snippet}}]}
    try:
        notion_update_page(page_id, props)
    except Exception as e:
        print(f"[ERR] pages.update failed: {e}")
    if snippet:
        try:
            notion_append_block(page_id, [{
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": snippet}}]}
            }])
        except Exception as e:
            print(f"[WARN] blocks.append failed: {e}")

# ---------- actions ----------
def fetch_ready_tasks():
    print("Querying Ready tasks…")
    res = notion.databases.query(
        database_id=DB_ID,
        filter={"property": "Status", "select": {"equals": "Ready"}},
        sorts=[
            {"property": "Priority", "direction": "ascending"},
            {"timestamp": "last_edited_time", "direction": "ascending"},
        ],
        page_size=20,
    )
    results = res.get("results", [])
    print(f"Found {len(results)} ready task(s).")
    return results

def safe_run(cmd: str | None):
    parts = shlex.split(cmd or "")
    if len(parts) != 1:
        raise RuntimeError("Only single command name allowed in Payload.cmd")
    name = os.path.basename(parts[0])
    if name not in ALLOWED_SCRIPTS:
        raise RuntimeError(f"Script '{name}' not allowed")
    print(f"safe_run: {name}")
    proc = subprocess.run(["/bin/bash", f"./tasks/{name}"], capture_output=True, text=True, timeout=300)
    out = (proc.stdout or "") + (proc.stderr or "")
    print(f"safe_run exit code: {proc.returncode}")
    return proc.returncode, out

def call_api(payload: dict):
    url = payload.get("url")
    method = (payload.get("method") or "GET").upper()
    body = payload.get("body")
    if not url or url not in ALLOWED_URLS:
        raise RuntimeError("URL not allowed")
    print(f"call_api: {url} {method}")
    r = requests.request(method, url, json=body, timeout=20)
    text = f"{r.status_code} {r.text[:1500]}"
    return r.status_code, text

def codex_apply(payload: dict):
    # CLI отключена — фейлим с понятной причиной, чтобы не висло
    raise RuntimeError("codex_apply disabled (Codex CLI not installed)")

def handle_task(page: dict):
    page_id = page["id"]
    title = page["properties"]["Name"]["title"][0]["plain_text"] if page["properties"]["Name"]["title"] else "(no title)"
    props = page["properties"]
    action = props["Action"]["select"]["name"] if props.get("Action") and props["Action"]["select"] else None
    payload_txt = "".join([t["plain_text"] for t in props.get("Payload", {}).get("rich_text", [])]) or "{}"
    print(f"Handling: {title} [{page_id}] action={action} payload={payload_txt}")

    try:
        payload = json.loads(payload_txt)
    except Exception:
        payload = {}

    set_status(page_id, "Running")
    try:
        if action == "run_script":
            code, out = safe_run(payload.get("cmd"))
        elif action == "call_api":
            code, out = call_api(payload)
        elif action == "codex_apply":
            code, out = codex_apply(payload)
        else:
            raise RuntimeError(f"Unknown or not allowed action: {action}")

        if (isinstance(code, int) and code == 0) or (isinstance(code, int) and 200 <= code < 300):
            set_status(page_id, "Done", out)
        else:
            set_status(page_id, "Failed", out)
    except Exception as e:
        set_status(page_id, "Failed", f"Error: {e}")

# ---------- EPIC: detect & decompose ----------
def fetch_ready_epics() -> List[dict]:
    print("Querying Ready epics…")
    res = notion.databases.query(
        database_id=DB_ID,
        filter={
            "and": [
                {"property": "Type", "select": {"equals": "Epic"}},
                {"property": "Status", "select": {"equals": "Ready"}},
            ]
        },
        sorts=[{"timestamp": "last_edited_time", "direction": "ascending"}],
        page_size=3,
    )
    results = res.get("results", [])
    print(f"Found {len(results)} ready epic(s).")
    return results

EPIC_PROMPT = """Ты — помощник по управлению задачами. Получишь описание большой цели (эпика).
Сформируй список атомарных задач в JSON-массиве (без пояснений вокруг), каждая задача — объект с полями:
- title: кратко-глаголом
- action: одно из ["run_script","call_api"]  # не используй codex_apply
- payload: минимальный JSON под действие (для run_script: {"cmd":"build.sh"}; для call_api: {"url":"https://httpbin.org/post","method":"POST","body":{...}})
- priority: целое число 1..N (1 — самый высокий)

Требования:
- 8–20 задач, 1 действие = 1 задача.
- Не добавляй секреты. URL — только из allow-list: https://httpbin.org/post
- Для сборки/проверки используй run_script с существующим build.sh
- Ответ — ТОЛЬКО JSON-массив без текста до/после.

Описание эпика:
"""

def llm_decompose_epic(description: str) -> List[Dict[str, Any]]:
    # читаем ключ "на лету", логируем наличие
    key = os.environ.get("OPENAI_API_KEY")
    print("LLM: OPENAI key present:", "yes" if bool(key) else "no")
    if not key:
        raise RuntimeError("OPENAI_API_KEY not set (LLM unavailable)")

    prompt = EPIC_PROMPT + description.strip()

    # Прямой HTTP-вызов Chat Completions
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "You generate only valid JSON arrays."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 1200,
    }

    r = requests.post(url, headers=headers, json=payload, timeout=90)
    if not r.ok:
        raise RuntimeError(f"OpenAI API error: {r.status_code} {r.text[:400]}")

    # ---- парсим ответ: срезаем ```json … ``` если LLM так вернул
    content = r.json()["choices"][0]["message"]["content"].strip()
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", content, re.DOTALL | re.IGNORECASE)
    if fenced:
        content = fenced.group(1).strip()
    else:
        m = re.search(r"\[\s*{.*}\s*\]", content, re.DOTALL)
        if m:
            content = m.group(0).strip()

    try:
        data = json.loads(content)
        if not isinstance(data, list):
            raise ValueError("Expected a JSON array")
    except Exception as e:
        raise RuntimeError(f"LLM JSON parse error: {e}  RAW={content[:400]}")

    # валидация под allow-листы
    tasks: List[Dict[str, Any]] = []
    p = 1
    for item in data[:25]:
        title = str(item.get("title") or "").strip()[:180]
        action = (item.get("action") or "").strip()
        pl = item.get("payload") or {}
        priority = int(item.get("priority") or p)
        if not title or action not in {"run_script", "call_api"}:
            continue

        if action == "run_script":
            cmd = str(pl.get("cmd") or "build.sh")
            if cmd not in ALLOWED_SCRIPTS:
                cmd = "build.sh"
            pl = {"cmd": cmd}
        else:
            url2 = str(pl.get("url") or "https://httpbin.org/post")
            if url2 not in ALLOWED_URLS:
                url2 = "https://httpbin.org/post"
            method = (pl.get("method") or "POST").upper()
            body = pl.get("body") or {"ping": "ok"}
            pl = {"url": url2, "method": method, "body": body}

        tasks.append({
            "title": title,
            "action": action,
            "payload": pl,
            "priority": max(1, min(999, priority))
        })
        p += 1

    if len(tasks) < 5:
        raise RuntimeError("Too few tasks after validation")

    return tasks

def create_tasks_in_notion(tasks: List[Dict[str, Any]]) -> int:
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    created = 0
    for t in tasks:
        properties = {
            "Name": {"title": [{"text": {"content": t["title"]}}]},
            "Type": {"select": {"name": "Task"}},
            "Status": {"select": {"name": "Draft"}},
            "Action": {"select": {"name": t["action"]}},
            "Payload": {"rich_text": [{"text": {"content": json.dumps(t["payload"], ensure_ascii=False)}}]},
            "Priority": {"number": t["priority"]},
        }
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json={"parent": {"database_id": DB_ID}, "properties": properties},
            timeout=20
        )
        if r.ok:
            created += 1
        else:
            print(f"[ERR] create page failed: {r.status_code} {r.text[:300]}")
    return created

def process_epics():
    epics = fetch_ready_epics()
    for epic in epics:
        epic_id = epic["id"]
        name = epic["properties"]["Name"]["title"][0]["plain_text"] if epic["properties"]["Name"]["title"] else "Epic"
        desc = "".join([t["plain_text"] for t in epic["properties"].get("Description", {}).get("rich_text", [])]).strip()
        print(f"Epic: {name} [{epic_id}] — decompose")
        if not desc:
            set_status(epic_id, "Failed", "Epic has empty Description")
            continue
        set_status(epic_id, "Running", "Decomposing epic into tasks...")
        try:
            tasks = llm_decompose_epic(desc)
            n = create_tasks_in_notion(tasks)
            set_status(epic_id, "Done", f"Created {n} task(s) from epic")
        except Exception as e:
            set_status(epic_id, "Failed", f"Epic decomposition failed: {e}")

# ---------- main ----------
def main():
    print("Starting worker…")
    print(f"DB_ID: {DB_ID}")
    print(f"OPENAI_API_KEY present: {'yes' if bool(os.environ.get('OPENAI_API_KEY')) else 'no'}")
    debug_dump_db_schema()

    # 1) сначала обработать эпики (если есть)
    try:
        process_epics()
    except Exception as e:
        print(f"[WARN] process_epics failed: {e}")

    # 2) затем выполнить готовые задачи
    tasks = fetch_ready_tasks()
    for i, t in enumerate(tasks, 1):
        print(f"Task {i}/{len(tasks)}")
        handle_task(t)
        time.sleep(1.0)

    print("Worker finished.")

if __name__ == "__main__":
    main()
