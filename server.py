"""
MyNet Server — для Render.com
HTTP + WebSocket на одном порту
"""

import asyncio
import json
import os
import base64
import hashlib
import datetime
from pathlib import Path
from aiohttp import web
import aiohttp

# ───────────────────────────────────────────────
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8765))
PASSWORD = os.environ.get("PASSWORD", "")  # ← ИЗМЕНИТЕ ЭТО!
UPLOAD_DIR = Path("uploads")
# ───────────────────────────────────────────────

UPLOAD_DIR.mkdir(exist_ok=True)
clients = {}


def log(msg: str):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


async def broadcast(message: dict, exclude=None):
    data = json.dumps(message, ensure_ascii=False)
    for ws, info in list(clients.items()):
        if ws != exclude and info.get("auth"):
            try:
                await ws.send_str(data)
            except Exception:
                pass


async def handle_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    clients[ws] = {"name": "unknown", "auth": False}
    addr = request.remote
    log(f"Подключение: {addr}")

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    await ws.send_str(json.dumps({"type": "error", "text": "Неверный формат"}))
                    continue

                mtype = data.get("type")

                if mtype == "auth":
                    if data.get("password") == PASSWORD:
                        name = data.get("name", f"PC-{addr}")
                        clients[ws]["auth"] = True
                        clients[ws]["name"] = name
                        await ws.send_str(json.dumps({"type": "auth_ok", "text": f"Добро пожаловать, {name}!"}))
                        log(f"Авторизован: {name}")
                        await broadcast({"type": "system", "text": f"🟢 {name} подключился"}, exclude=ws)
                    else:
                        await ws.send_str(json.dumps({"type": "auth_fail", "text": "Неверный пароль"}))
                    continue

                if not clients[ws]["auth"]:
                    await ws.send_str(json.dumps({"type": "error", "text": "Сначала авторизуйтесь"}))
                    continue

                name = clients[ws]["name"]

                if mtype == "list":
                    pc_list = [info["name"] for w, info in clients.items() if info.get("auth")]
                    await ws.send_str(json.dumps({"type": "list", "clients": pc_list}))

                elif mtype == "chat":
                    text = data.get("text", "")
                    to = data.get("to")
                    log(f"Чат [{name}]: {text}")
                    payload = {"type": "chat", "from": name, "text": text}
                    if to:
                        for w, info in clients.items():
                            if info["name"] == to and info["auth"]:
                                await w.send_str(json.dumps(payload))
                                break
                    else:
                        await broadcast(payload, exclude=ws)

                elif mtype == "file_send":
                    filename = os.path.basename(data.get("filename", "file.bin"))
                    to = data.get("to")
                    file_bytes = base64.b64decode(data.get("data", ""))
                    size_kb = len(file_bytes) / 1024
                    if to:
                        sent = False
                        for w, info in clients.items():
                            if info["name"] == to and info["auth"]:
                                await w.send_str(json.dumps({
                                    "type": "file_receive",
                                    "from": name,
                                    "filename": filename,
                                    "data": data.get("data", ""),
                                    "size_kb": round(size_kb, 1)
                                }))
                                sent = True
                                break
                        if sent:
                            await ws.send_str(json.dumps({"type": "ok", "text": f"Файл '{filename}' отправлен → {to}"}))
                        else:
                            await ws.send_str(json.dumps({"type": "error", "text": f"ПК '{to}' не найден"}))
                    else:
                        (UPLOAD_DIR / filename).write_bytes(file_bytes)
                        await ws.send_str(json.dumps({"type": "ok", "text": f"Файл '{filename}' сохранён ({size_kb:.1f} KB)"}))
                        log(f"Файл сохранён: {filename}")

                elif mtype == "file_list":
                    files = [{"name": f.name, "size_kb": round(f.stat().st_size / 1024, 1)}
                             for f in UPLOAD_DIR.iterdir() if f.is_file()]
                    await ws.send_str(json.dumps({"type": "file_list", "files": files}))

                elif mtype == "file_get":
                    filename = os.path.basename(data.get("filename", ""))
                    file_path = UPLOAD_DIR / filename
                    if file_path.exists():
                        d = base64.b64encode(file_path.read_bytes()).decode()
                        await ws.send_str(json.dumps({
                            "type": "file_receive", "from": "server",
                            "filename": filename, "data": d,
                            "size_kb": round(file_path.stat().st_size / 1024, 1)
                        }))
                    else:
                        await ws.send_str(json.dumps({"type": "error", "text": f"Файл '{filename}' не найден"}))

            elif msg.type == aiohttp.WSMsgType.ERROR:
                break

    finally:
        name = clients[ws]["name"]
        auth = clients[ws]["auth"]
        del clients[ws]
        if auth:
            log(f"Отключился: {name}")
            await broadcast({"type": "system", "text": f"🔴 {name} отключился"})

    return ws


async def handle_http(request):
    return web.Response(text="MyNet Server is running ✅")


app = web.Application()
app.router.add_get("/", handle_http)
app.router.add_get("/ws", handle_ws)

if __name__ == "__main__":
    log(f"MyNet Server запущен на порту {PORT}")
    web.run_app(app, host=HOST, port=PORT)