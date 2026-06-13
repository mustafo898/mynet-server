"""
MyNet Server — запускается на VPS
Функции: передача файлов, выполнение команд, чат между ПК
"""

import asyncio
import websockets
import json
import os
import base64
import hashlib
import datetime
from pathlib import Path

# ───────────────────────────────────────────────
# НАСТРОЙКИ — измените под себя
HOST = "0.0.0.0"       # слушать все интерфейсы
PORT = 8765            # порт (откройте его в файрволе VPS)
PASSWORD = "your_secret_password_here"  # ← ИЗМЕНИТЕ ЭТО!
UPLOAD_DIR = Path("uploads")  # папка для файлов
# ───────────────────────────────────────────────

UPLOAD_DIR.mkdir(exist_ok=True)

# Подключённые клиенты: {websocket: {"name": str, "auth": bool}}
clients = {}


def log(msg: str):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


async def broadcast(message: dict, exclude=None):
    """Отправить сообщение всем авторизованным клиентам"""
    data = json.dumps(message, ensure_ascii=False)
    for ws, info in clients.items():
        if ws != exclude and info.get("auth"):
            try:
                await ws.send(data)
            except Exception:
                pass


async def handle_client(websocket):
    clients[websocket] = {"name": "unknown", "auth": False}
    addr = websocket.remote_address
    log(f"Подключение: {addr}")

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({"type": "error", "text": "Неверный формат"}))
                continue

            mtype = msg.get("type")

            # ── Авторизация ──────────────────────────────
            if mtype == "auth":
                if msg.get("password") == PASSWORD:
                    name = msg.get("name", f"PC-{addr[0]}")
                    clients[websocket]["auth"] = True
                    clients[websocket]["name"] = name
                    await websocket.send(json.dumps({"type": "auth_ok", "text": f"Добро пожаловать, {name}!"}))
                    log(f"Авторизован: {name} ({addr[0]})")
                    await broadcast({"type": "system", "text": f"🟢 {name} подключился"}, exclude=websocket)
                else:
                    await websocket.send(json.dumps({"type": "auth_fail", "text": "Неверный пароль"}))
                    log(f"Неверный пароль от {addr[0]}")
                continue

            # Все остальные команды — только для авторизованных
            if not clients[websocket]["auth"]:
                await websocket.send(json.dumps({"type": "error", "text": "Сначала авторизуйтесь"}))
                continue

            name = clients[websocket]["name"]

            # ── Список подключённых ПК ───────────────────
            if mtype == "list":
                pc_list = [info["name"] for ws, info in clients.items() if info.get("auth")]
                await websocket.send(json.dumps({"type": "list", "clients": pc_list}))

            # ── Чат сообщение ────────────────────────────
            elif mtype == "chat":
                text = msg.get("text", "")
                to = msg.get("to")  # None = всем
                log(f"Чат [{name}]: {text}")
                payload = {"type": "chat", "from": name, "text": text}
                if to:
                    # личное сообщение конкретному ПК
                    for ws, info in clients.items():
                        if info["name"] == to and info["auth"]:
                            await ws.send(json.dumps(payload))
                            break
                else:
                    await broadcast(payload, exclude=websocket)

            # ── Отправка файла ────────────────────────────
            elif mtype == "file_send":
                filename = os.path.basename(msg.get("filename", "file.bin"))
                to = msg.get("to")  # имя получателя или None = сохранить на сервере
                data_b64 = msg.get("data", "")
                file_bytes = base64.b64decode(data_b64)
                size_kb = len(file_bytes) / 1024

                if to:
                    # Переслать другому ПК напрямую
                    sent = False
                    for ws, info in clients.items():
                        if info["name"] == to and info["auth"]:
                            await ws.send(json.dumps({
                                "type": "file_receive",
                                "from": name,
                                "filename": filename,
                                "data": data_b64,
                                "size_kb": round(size_kb, 1)
                            }))
                            sent = True
                            break
                    if sent:
                        await websocket.send(json.dumps({"type": "ok", "text": f"Файл '{filename}' отправлен → {to}"}))
                        log(f"Файл {filename} ({size_kb:.1f} KB): {name} → {to}")
                    else:
                        await websocket.send(json.dumps({"type": "error", "text": f"ПК '{to}' не найден"}))
                else:
                    # Сохранить на сервере
                    save_path = UPLOAD_DIR / filename
                    save_path.write_bytes(file_bytes)
                    md5 = hashlib.md5(file_bytes).hexdigest()[:8]
                    await websocket.send(json.dumps({"type": "ok", "text": f"Файл '{filename}' сохранён на сервере ({size_kb:.1f} KB)"}))
                    log(f"Файл сохранён: {filename} ({size_kb:.1f} KB) от {name} [md5:{md5}]")

            # ── Список файлов на сервере ──────────────────
            elif mtype == "file_list":
                files = []
                for f in UPLOAD_DIR.iterdir():
                    if f.is_file():
                        files.append({"name": f.name, "size_kb": round(f.stat().st_size / 1024, 1)})
                await websocket.send(json.dumps({"type": "file_list", "files": files}))

            # ── Скачать файл с сервера ────────────────────
            elif mtype == "file_get":
                filename = os.path.basename(msg.get("filename", ""))
                file_path = UPLOAD_DIR / filename
                if file_path.exists():
                    data_b64 = base64.b64encode(file_path.read_bytes()).decode()
                    await websocket.send(json.dumps({
                        "type": "file_receive",
                        "from": "server",
                        "filename": filename,
                        "data": data_b64,
                        "size_kb": round(file_path.stat().st_size / 1024, 1)
                    }))
                    log(f"Скачан файл: {filename} → {name}")
                else:
                    await websocket.send(json.dumps({"type": "error", "text": f"Файл '{filename}' не найден"}))

            else:
                await websocket.send(json.dumps({"type": "error", "text": f"Неизвестная команда: {mtype}"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        name = clients[websocket]["name"]
        auth = clients[websocket]["auth"]
        del clients[websocket]
        if auth:
            log(f"Отключился: {name}")
            await broadcast({"type": "system", "text": f"🔴 {name} отключился"})


async def main():
    log(f"MyNet Server запущен на порту {PORT}")
    log(f"Папка для файлов: {UPLOAD_DIR.absolute()}")
    log("Ожидание подключений...")
    async with websockets.serve(handle_client, HOST, PORT):
        await asyncio.Future()  # работать вечно


if __name__ == "__main__":
    asyncio.run(main())
