import asyncio
import urllib
import json


class UrllibSSEClient:
    def __init__(self, base_url: str = "http://localhost:8000", nickname: str = "Desktop_PC"):
        self.base_url = base_url
        self.nickname = nickname

    def create_kernel(self) -> str:
        """ Create a new kernel using urllib """
        url = f"{self.base_url}/kernels"
        req = urllib.request.Request(url, method='POST')

        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            return data["kernel_id"]

    async def execute_code_async(self, kernel_id: str, code: str):
        """ Execute code in the kernel and return events using SSE """
        url = f"{self.base_url}/kernels/{kernel_id}/cells?nickname={self.nickname}"
        post_data = json.dumps({"code": code}).encode()

        req = urllib.request.Request(
            url,
            data=post_data,
            headers={
                'Content-Type': "application/json",
                'Accept': "text/event-stream"
            },
            method="POST"
        )

        loop = asyncio.get_event_loop()

        def _make_request():
            with urllib.request.urlopen(req) as response:
                events = []
                for line in response:
                    line = line.decode('utf-8').strip()
                    if line.startswith('data: '):
                        try:
                            event_data = json.loads(line[6:])
                            events.append(event_data)
                            if event_data.get('status') == 'complete':
                                break
                        except json.JSONDecodeError:
                            continue
                return events

        return await loop.run_in_executor(None, _make_request)


if __name__ == '__main__':
    client = UrllibSSEClient()
    kernel_id = client.create_kernel()
    events = client.execute_code_async(kernel_id, "print('Hello, World!')")
    for event in events:
        print(event)
