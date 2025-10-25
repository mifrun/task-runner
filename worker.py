import os, json, shlex, subprocess
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]
notion = Client(auth=NOTION_TOKEN)

ALLOWED_ACTIONS = {"run_script", "call_api", "codex_apply"}
ALLOWED_SCRIPTS = {"build.sh", "sync_data.sh"}
ALLOWED_URLS = {"https://example.com/hook"}

def set_status(page_id, status, logs=None):
    notion.pages.update(page_id=page_id, properties={"Status": {"select": {"name": status}}})
    if logs:
        notion.blocks.children.append(page_id, children=[{
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type":"text","text":{"content": str(logs)[:1800]}}]}
        }])

def fetch_ready_tasks():
    res = notion.databases.query(
        database_id=DB_ID,
        filter={"property":"Status","select":{"equals":"Ready"}},
        page_size=10
    )
    return res.get("results", [])

def safe_run(cmd):
    parts = shlex.split(cmd or "")
    if len(parts) != 1:
        raise RuntimeError("Only single command name allowed")
    name = os.path.basename(parts[0])
    if name not in ALLOWED_SCRIPTS:
        raise RuntimeError(f"Script '{name}' not allowed")
    proc = subprocess.run(["/bin/bash", f"./tasks/{name}"], capture_output=True, text=True, timeout=300)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")

def call_api(payload):
    import requests
    url = payload.get("url"); method = (payload.get("method") or "GET").upper()
    body = payload.get("body")
    if not url or url not in ALLOWED_URLS:
        raise RuntimeError("URL not allowed")
    r = requests.request(method, url, json=body, timeout=20)
    return r.status_code, f"{r.status_code} {r.text[:1500]}"

def codex_apply(payload):
    spec = (payload.get("spec") or "").strip()
    if not spec: raise RuntimeError("Missing spec for codex_apply")
    repo = payload.get("repo_path", "."); to = int(payload.get("timeout_sec", 1800))
    proc = subprocess.run(["codex","apply","--repo",repo,"--spec",spec,"--yes"],
                          capture_output=True, text=True, timeout=to)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")

def handle_task(page):
    page_id = page["id"]; props = page["properties"]
    action = props["Action"]["select"]["name"] if props.get("Action") and props["Action"]["select"] else None
    payload_txt = "".join([t["plain_text"] for t in props.get("Payload",{}).get("rich_text",[])]) or "{}"
    try: payload = json.loads(payload_txt)
    except Exception: payload = {}

    if action not in ALLOWED_ACTIONS:
        set_status(page_id,"Failed",f"Action '{action}' not allowed"); return

    set_status(page_id,"Running")
    try:
        if action == "run_script": code,out = safe_run(payload.get("cmd"))
        elif action == "call_api": code,out = call_api(payload)
        elif action == "codex_apply": code,out = codex_apply(payload)
        else: raise RuntimeError("Unknown action")
        if (isinstance(code,int) and code == 0) or (200 <= code < 300):
            set_status(page_id,"Done",out)
        else:
            set_status(page_id,"Failed",out)
    except Exception as e:
        set_status(page_id,"Failed",f"Error: {e}")

def main():
    for task in fetch_ready_tasks():
        handle_task(task)

if __name__ == "__main__":
    main()
