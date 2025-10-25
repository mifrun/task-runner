def llm_decompose_epic(description: str) -> list[dict]:
    import os, json, requests

    key = os.environ.get("OPENAI_API_KEY")  # читаем из env на лету
    print("LLM: OPENAI key present:", "yes" if bool(key) else "no")
    if not key:
        raise RuntimeError("OPENAI_API_KEY not set (LLM unavailable)")

    prompt = EPIC_PROMPT + description.strip()

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

    try:
        content = r.json()["choices"][0]["message"]["content"].strip()
        data = json.loads(content)
        if not isinstance(data, list):
            raise ValueError("Expected a JSON array")
    except Exception as e:
        raise RuntimeError(f"LLM JSON parse error: {e}  RAW={content[:400]}")

    # валидация под allow-листы
    tasks = []
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
            if cmd not in ALLOWED_SCRIPTS: cmd = "build.sh"
            pl = {"cmd": cmd}
        else:
            url2 = str(pl.get("url") or "https://httpbin.org/post")
            if url2 not in ALLOWED_URLS: url2 = "https://httpbin.org/post"
            method = (pl.get("method") or "POST").upper()
            body = pl.get("body") or {"ping": "ok"}
            pl = {"url": url2, "method": method, "body": body}

        tasks.append({"title": title, "action": action, "payload": pl, "priority": max(1, min(999, priority))})
        p += 1

    if len(tasks) < 5:
        raise RuntimeError("Too few tasks after validation")

    return tasks
