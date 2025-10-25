# decompose_test_retry.py (можешь сохранить как decompose_test.py)
import os, json, requests, sys, time

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("ERROR: set OPENAI_API_KEY env var"); sys.exit(1)

EPIC_DESCRIPTION = """Сделать самый простой MVP онбординга:
— Экран логина и экран регистрации на одной странице.
— Регистрация: email + пароль, базовая валидация.
— Логин: email + пароль.
— После логина показывать заглушку “Вы вошли”.
— Скрипт сборки и один тест, который проверяет успешную регистрацию.
"""

EPIC_PROMPT = """Ты — помощник по управлению задачами. Получишь описание большой цели (эпика).
Сформируй список атомарных задач в JSON-массиве (без пояснений вокруг), каждая задача — объект с полями:
- title
- action: ["run_script","call_api"]  # не используй codex_apply
- payload
- priority: 1..N
Требования: 8–20 задач; URL только https://httpbin.org/post; run_script -> {"cmd":"build.sh"}.
Ответ — ТОЛЬКО JSON-массив.
Описание эпика:
"""

def chat_complete(prompt: str) -> str:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "You generate only valid JSON arrays."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 1200,
    }
    # ретраи при 5xx/сетевых ошибках
    last_err = None
    for i in range(5):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=90)
            if r.ok:
                return r.json()["choices"][0]["message"]["content"].strip()
            else:
                # 520 и подобные — подождать и повторить
                last_err = f"{r.status_code} {r.text[:300]}"
                print(f"[attempt {i+1}/5] OpenAI API error: {last_err}")
                if r.status_code >= 500:
                    time.sleep(2 ** i)
                else:
                    break
        except requests.RequestException as e:
            last_err = str(e)
            print(f"[attempt {i+1}/5] Network error: {last_err}")
            time.sleep(2 ** i)
    raise SystemExit(f"Failed after retries: {last_err}")

def main():
    content = chat_complete(EPIC_PROMPT + EPIC_DESCRIPTION.strip())
    try:
        data = json.loads(content)
        tasks = []
        p = 1
        ALLOWED_SCRIPTS = {"build.sh"}
        ALLOWED_URLS = {"https://httpbin.org/post"}
        for item in data[:25]:
            title = str(item.get("title") or "").strip()[:180]
            action = (item.get("action") or "").strip()
            pl = item.get("payload") or {}
            priority = int(item.get("priority") or p)
            if not title or action not in {"run_script","call_api"}:
                continue
            if action == "run_script":
                cmd = str(pl.get("cmd") or "build.sh")
                if cmd not in ALLOWED_SCRIPTS: cmd = "build.sh"
                pl = {"cmd": cmd}
            else:
                url = str(pl.get("url") or "https://httpbin.org/post")
                if url not in ALLOWED_URLS: url = "https://httpbin.org/post"
                method = (pl.get("method") or "POST").upper()
                body = pl.get("body") or {"ping": "ok"}
                pl = {"url": url, "method": method, "body": body}
            tasks.append({"title": title, "action": action, "payload": pl, "priority": max(1, min(999, priority))})
            p += 1
        if len(tasks) < 5:
            raise ValueError("Too few tasks")
        print(json.dumps(tasks, ensure_ascii=False, indent=2))
    except Exception as e:
        print("Parse error:", e)
        print("RAW:", content[:1000])

if __name__ == "__main__":
    main()
