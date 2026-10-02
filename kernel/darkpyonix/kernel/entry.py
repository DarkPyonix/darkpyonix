from __future__ import annotations
import asyncio, os, json, sys, base64, socket, glob

KERNEL_ID = os.environ.get("DARKPYONIX_KERNEL_ID")
FD_PASS_PATH = os.environ.get("FD_PASS_PATH")  # POSIX only


async def windows_share_poll_loop():
    if os.name != 'nt':
        return
    while True:
        for path in glob.glob(f"_adopt_{KERNEL_ID}_*.sockshare"):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception:
                continue
            try:
                share_b64 = data.get('share'); token = data.get('token')
                raw = base64.b64decode(share_b64)
                s = socket.fromshare(raw)
            except Exception as e:
                print('share open failed', e, file=sys.stderr)
                os.remove(path)
                continue
            try:
                headers = (
                    "HTTP/1.1 200 OK\r\n"
                    "Content-Type: text/event-stream\r\n"
                    "Cache-Control: no-cache\r\n"
                    "Connection: keep-alive\r\n\r\n"
                )
                s.sendall(headers.encode())
                for i in range(5):
                    payload = {"kernel_id": KERNEL_ID, "token": token, "i": i}
                    event = f"event: message\ndata: {json.dumps(payload)}\n\n".encode()
                    s.sendall(event)
                    await asyncio.sleep(0.05)
            finally:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                s.close()
                try:
                    os.remove(path)
                except Exception:
                    pass
        await asyncio.sleep(0.2)


async def posix_fd_pass_loop():
    if os.name == 'nt' or not FD_PASS_PATH:
        return
    try:
        reader, writer = await asyncio.open_unix_connection(FD_PASS_PATH)
    except Exception as e:
        print("fd pass connect failed", e, file=sys.stderr)
        return
    writer.write((KERNEL_ID + "\n").encode())
    await writer.drain()
    raw = writer.get_extra_info("socket")
    raw.setblocking(True)
    loop = asyncio.get_event_loop()
    while True:
        try:
            hdr = await loop.run_in_executor(None, raw.recv, 4)
            if not hdr:
                break
            meta_len = int.from_bytes(hdr, 'big')
            meta_bytes = b''
            while len(meta_bytes) < meta_len:
                meta_bytes += await loop.run_in_executor(None, raw.recv, meta_len - len(meta_bytes))
            data, anc, *_ = raw.recvmsg(1, socket.CMSG_LEN(4))
            fd = None
            for c_level, c_type, c_data in anc:
                if c_level == socket.SOL_SOCKET and c_type == socket.SCM_RIGHTS:
                    fd = int.from_bytes(c_data[:4], 'little')
            if fd is None:
                continue
            client_sock = socket.socket(fileno=fd)
            try:
                headers = (
                    "HTTP/1.1 200 OK\r\n"
                    "Content-Type: text/event-stream\r\n"
                    "Cache-Control: no-cache\r\n"
                    "Connection: keep-alive\r\n\r\n"
                )
                client_sock.sendall(headers.encode())
                token = json.loads(meta_bytes.decode()).get("token")
                for i in range(5):
                    payload = {"kernel_id": KERNEL_ID, "token": token, "i": i}
                    event = f"event: message\ndata: {json.dumps(payload)}\n\n".encode()
                    client_sock.sendall(event)
                    await asyncio.sleep(0.05)
            finally:
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                client_sock.close()
        except Exception as e:
            print("fd pass loop error", e, file=sys.stderr)
            break


async def main():
    await asyncio.gather(windows_share_poll_loop(), posix_fd_pass_loop())


if __name__ == "__main__":
    asyncio.run(main())
