# SSE 기반 FD 전달 방식 테스트 및 사용 예제

import asyncio
import aiohttp
import json
import requests
import time
from typing import Dict, Any, AsyncGenerator

# SSE 클라이언트
class SSEJupyterClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def create_kernel(self) -> str:
        """새 커널 생성 (동기)"""
        response = requests.post(f"{self.base_url}/kernels")
        response.raise_for_status()
        return response.json()["kernel_id"]

    async def execute_code_stream(self, kernel_id: str, code: str) -> AsyncGenerator[dict, None]:
        """코드 실행 및 SSE 스트림 수신 (FD가 커널로 직접 전달됨)"""
        url = f"{self.base_url}/kernels/{kernel_id}/execute"

        async with self.session.post(
                url,
                json={"code": code},
                headers={"Accept": "text/event-stream"}
        ) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status}: {await response.text()}")

            async for line in response.content:
                line = line.decode('utf-8').strip()
                if line.startswith('data: '):
                    try:
                        data = json.loads(line[6:])  # "data: " 제거
                        yield data
                    except json.JSONDecodeError:
                        continue

# 사용 예제
async def basic_usage_example():
    """기본 사용 예제"""
    print("=== Basic SSE FD Passing Example ===")

    async with SSEJupyterClient() as client:
        # 커널 생성
        kernel_id = client.create_kernel()
        print(f"Created kernel: {kernel_id}")

        # 코드 실행 (SSE 스트림)
        code = """
print("Hello from kernel!")
import math
result = math.sqrt(16)
print(f"Square root of 16 is: {result}")
result
"""

        print("Executing code...")
        async for event in client.execute_code_stream(kernel_id, code):
            print(f"Received: {event}")

# 성능 테스트
class SSEPerformanceTest:
    def __init__(self):
        self.base_url = "http://localhost:8000"

    async def test_connection_overhead(self, num_requests: int = 50):
        """SSE 연결 오버헤드 테스트"""
        print(f"Testing {num_requests} SSE connections...")

        # 커널 생성
        kernel_id = requests.post(f"{self.base_url}/kernels").json()["kernel_id"]

        async def single_request_test():
            start_time = time.time()
            async with SSEJupyterClient() as client:
                events = []
                async for event in client.execute_code_stream(kernel_id, "2+2"):
                    events.append(event)
                    if event.get('status') == 'complete':
                        break
            return time.time() - start_time, len(events)

        # 동시 요청 테스트
        start_time = time.time()
        tasks = [single_request_test() for _ in range(num_requests)]
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time

        request_times = [r[0] for r in results]
        event_counts = [r[1] for r in results]

        avg_request_time = sum(request_times) / len(request_times)
        avg_events = sum(event_counts) / len(event_counts)

        print(f"Average request time: {avg_request_time:.4f}s")
        print(f"Average events per request: {avg_events:.1f}")
        print(f"Total time for {num_requests} requests: {total_time:.4f}s")
        print(f"Requests per second: {num_requests / total_time:.2f}")

    async def test_streaming_performance(self):
        """스트리밍 성능 테스트"""
        print("Testing streaming performance...")

        kernel_id = requests.post(f"{self.base_url}/kernels").json()["kernel_id"]

        # 긴 실행 시간을 가진 코드
        long_running_code = """
import time
for i in range(10):
    print(f"Step {i+1}/10")
    time.sleep(0.1)
print("Complete!")
"""

        start_time = time.time()
        event_count = 0
        first_event_time = None

        async with SSEJupyterClient() as client:
            async for event in client.execute_code_stream(kernel_id, long_running_code):
                if first_event_time is None:
                    first_event_time = time.time()

                event_count += 1
                print(f"Event {event_count}: {event}")

                if event.get('status') == 'complete':
                    break

        end_time = time.time()

        total_time = end_time - start_time
        first_response_time = first_event_time - start_time if first_event_time else 0

        print(f"First response time: {first_response_time:.4f}s")
        print(f"Total execution time: {total_time:.4f}s")
        print(f"Total events received: {event_count}")

# 벤치마크 비교
class SSEBenchmarkComparison:
    """SSE FD 전달 vs 일반 HTTP 방식 성능 비교"""

    async def benchmark_fd_passing_sse(self, iterations: int = 100):
        """FD 전달 방식 SSE 벤치마크"""
        print("Benchmarking FD passing SSE...")

        kernel_id = requests.post("http://localhost:8000/kernels").json()["kernel_id"]

        start_time = time.time()

        async with SSEJupyterClient() as client:
            for i in range(iterations):
                async for event in client.execute_code_stream(kernel_id, f"result = {i} * 2"):
                    if event.get('status') == 'complete':
                        break

        end_time = time.time()

        total_time = end_time - start_time
        avg_time = total_time / iterations

        return {
            "method": "FD Passing SSE",
            "iterations": iterations,
            "total_time": total_time,
            "avg_time_per_request": avg_time,
            "requests_per_second": iterations / total_time
        }

    async def run_comparison(self):
        """성능 비교 실행"""
        print("Running SSE performance comparison...")

        fd_results = await self.benchmark_fd_passing_sse(50)

        print("\n=== SSE Benchmark Results ===")
        print(f"Method: {fd_results['method']}")
        print(f"Iterations: {fd_results['iterations']}")
        print(f"Total time: {fd_results['total_time']:.4f}s")
        print(f"Average time per request: {fd_results['avg_time_per_request']:.6f}s")
        print(f"Requests per second: {fd_results['requests_per_second']:.2f}")

# 실제 Jupyter 노트북 시뮬레이션
class JupyterNotebookSimulator:
    def __init__(self):
        self.base_url = "http://localhost:8000"

    async def simulate_notebook_session(self):
        """Jupyter 노트북 세션 시뮬레이션"""
        print("=== Simulating Jupyter Notebook Session ===")

        # 커널 생성
        kernel_id = requests.post(f"{self.base_url}/kernels").json()["kernel_id"]
        print(f"Started kernel: {kernel_id}")

        # 일련의 셀 실행
        cells = [
            "import numpy as np",
            "import pandas as pd",
            "data = np.random.randn(1000)",
            "df = pd.DataFrame({'values': data})",
            "print(df.describe())",
            "import matplotlib.pyplot as plt",
            "plt.hist(data, bins=30)",
            "plt.show()",
        ]

        async with SSEJupyterClient() as client:
            for i, cell_code in enumerate(cells, 1):
                print(f"\\nExecuting cell {i}: {cell_code}")

                async for event in client.execute_code_stream(kernel_id, cell_code):
                    if event.get('type') == 'stdout':
                        print(f"  Output: {event.get('text', '')}")
                    elif event.get('status') == 'complete':
                        print(f"  Cell {i} completed")
                        break

                await asyncio.sleep(0.5)  # 셀 간 지연

# 프로덕션 설정
class SSEProductionConfig:
    """SSE 기반 프로덕션 환경 설정"""

    @staticmethod
    def get_recommended_config():
        """추천 프로덕션 설정"""
        return {
            # FastAPI/Uvicorn 설정
            "fastapi": {
                "host": "0.0.0.0",
                "port": 8000,
                "workers": 1,  # FD 전달은 단일 프로세스에서만 작동
                "loop": "uvloop",  # Linux/macOS에서 성능 향상
                "access_log": False,  # 성능을 위해 비활성화
                "timeout_keep_alive": 65,  # SSE 연결 유지
                "timeout_graceful_shutdown": 30,
            },

            # SSE 설정
            "sse": {
                "keep_alive_interval": 30,  # 30초마다 keep-alive
                "client_timeout": 300,  # 5분 클라이언트 타임아웃
                "buffer_size": 8192,
                "max_concurrent_streams": 1000,
            },

            # 커널 관리 설정
            "kernel_manager": {
                "max_kernels": 100,
                "kernel_timeout": 3600,  # 1시간
                "cleanup_interval": 300,  # 5분마다 정리
                "kernel_restart_limit": 3,
            },

            # FD 전달 설정
            "fd_passing": {
                "socket_timeout": 30,
                "max_pending_fds": 1000,
                "fd_cleanup_interval": 60,
                "unix_socket_path": "/tmp/kernel_sockets",
            }
        }

    @staticmethod
    def setup_logging():
        """로깅 설정"""
        import logging

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('kernel_manager.log'),
                logging.StreamHandler()
            ]
        )

        # 성능에 영향을 주는 verbose 로깅 비활성화
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
        logging.getLogger("uvicorn.error").setLevel(logging.INFO)

# 모니터링 및 헬스 체크
class SSEMonitoring:
    """SSE 연결 모니터링"""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url

    async def health_check(self):
        """헬스 체크"""
        try:
            response = requests.get(f"{self.base_url}/kernels", timeout=5)
            return response.status_code == 200
        except:
            return False

    async def monitor_kernel_performance(self, kernel_id: str, duration: int = 60):
        """커널 성능 모니터링"""
        print(f"Monitoring kernel {kernel_id} for {duration} seconds...")

        start_time = time.time()
        request_count = 0
        error_count = 0
        response_times = []

        async with SSEJupyterClient() as client:
            while time.time() - start_time < duration:
                try:
                    req_start = time.time()

                    async for event in client.execute_code_stream(kernel_id, "2+2"):
                        if event.get('status') == 'complete':
                            break

                    response_time = time.time() - req_start
                    response_times.append(response_time)
                    request_count += 1

                except Exception as e:
                    error_count += 1
                    print(f"Request error: {e}")

                await asyncio.sleep(1)

        # 통계 계산
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        success_rate = (request_count / (request_count + error_count)) * 100 if (request_count + error_count) > 0 else 0

        print(f"Performance Report:")
        print(f"  Total requests: {request_count}")
        print(f"  Errors: {error_count}")
        print(f"  Success rate: {success_rate:.1f}%")
        print(f"  Average response time: {avg_response_time:.4f}s")
        print(f"  Min response time: {min(response_times):.4f}s" if response_times else "N/A")
        print(f"  Max response time: {max(response_times):.4f}s" if response_times else "N/A")

# 부하 테스트
class SSELoadTest:
    """SSE 부하 테스트"""

    async def concurrent_connections_test(self, num_connections: int = 50):
        """동시 연결 부하 테스트"""
        print(f"Starting load test with {num_connections} concurrent connections...")

        # 커널들 미리 생성
        kernel_ids = []
        for i in range(min(10, num_connections)):  # 최대 10개 커널
            response = requests.post("http://localhost:8000/kernels")
            kernel_ids.append(response.json()["kernel_id"])

        async def worker(worker_id: int):
            """개별 워커"""
            kernel_id = kernel_ids[worker_id % len(kernel_ids)]

            try:
                async with SSEJupyterClient() as client:
                    start_time = time.time()

                    async for event in client.execute_code_stream(
                            kernel_id,
                            f"import time; time.sleep(0.1); result = {worker_id} * 10"
                    ):
                        if event.get('status') == 'complete':
                            break

                    return time.time() - start_time

            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                return None

        # 워커들 실행
        start_time = time.time()
        tasks = [worker(i) for i in range(num_connections)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_time = time.time() - start_time

        # 결과 분석
        successful_results = [r for r in results if isinstance(r, float)]
        error_count = len(results) - len(successful_results)

        if successful_results:
            avg_time = sum(successful_results) / len(successful_results)
            print(f"Load test completed:")
            print(f"  Total time: {total_time:.2f}s")
            print(f"  Successful connections: {len(successful_results)}/{num_connections}")
            print(f"  Error rate: {(error_count/num_connections)*100:.1f}%")
            print(f"  Average response time: {avg_time:.4f}s")
            print(f"  Throughput: {len(successful_results)/total_time:.2f} req/s")

# 도커 배포를 위한 설정
DOCKERFILE_SSE = '''
FROM python:3.11-slim

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y \\
    build-essential \\
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . /app
WORKDIR /app

# Unix Domain Socket 디렉토리 생성
RUN mkdir -p /tmp/kernel_sockets

# 권한 설정
RUN chmod 755 /tmp/kernel_sockets

# 포트 노출
EXPOSE 8000

# 헬스체크 추가
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD curl -f http://localhost:8000/kernels || exit 1

# 애플리케이션 실행
CMD ["python", "main.py"]
'''

REQUIREMENTS_SSE = '''
fastapi==0.104.1
uvicorn[standard]==0.24.0
aiohttp==3.9.1
uvloop==0.19.0  # Linux/macOS 성능 향상
pydantic==2.5.0
'''

# 클라이언트 예제 (JavaScript)
CLIENT_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>SSE Jupyter Client</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 800px; margin: 0 auto; }
        textarea { width: 100%; height: 200px; font-family: monospace; }
        button { padding: 10px 20px; margin: 10px 0; }
        .output { background: #f5f5f5; padding: 15px; margin: 10px 0; border-radius: 5px; }
        .event { margin: 5px 0; padding: 5px; background: white; border-left: 3px solid #007acc; }
    </style>
</head>
<body>
    <div class="container">
        <h1>SSE Jupyter Client</h1>
        
        <button onclick="createKernel()">Create Kernel</button>
        <span id="kernelStatus">No kernel</span>
        
        <h3>Code Input:</h3>
        <textarea id="codeInput" placeholder="Enter Python code here...">
print("Hello from SSE!")
import math
result = math.sqrt(25)
print(f"Square root of 25 is: {result}")
result
        </textarea>
        
        <br>
        <button onclick="executeCode()" id="executeBtn">Execute Code</button>
        <button onclick="clearOutput()">Clear Output</button>
        
        <h3>Output:</h3>
        <div id="output" class="output"></div>
    </div>

    <script>
        let currentKernelId = null;
        
        async function createKernel() {
            try {
                const response = await fetch('/kernels', { method: 'POST' });
                const data = await response.json();
                currentKernelId = data.kernel_id;
                document.getElementById('kernelStatus').textContent = `Kernel: ${currentKernelId}`;
            } catch (error) {
                console.error('Error creating kernel:', error);
            }
        }
        
        async function executeCode() {
            if (!currentKernelId) {
                alert('Please create a kernel first');
                return;
            }
            
            const code = document.getElementById('codeInput').value;
            const executeBtn = document.getElementById('executeBtn');
            const output = document.getElementById('output');
            
            executeBtn.disabled = true;
            executeBtn.textContent = 'Executing...';
            
            try {
                const response = await fetch(`/kernels/${currentKernelId}/execute`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'text/event-stream'
                    },
                    body: JSON.stringify({ code: code })
                });
                
                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    
                    const chunk = decoder.decode(value);
                    const lines = chunk.split('\\n');
                    
                    for (const line of lines) {
                        if (line.startsWith('data: ')) {
                            try {
                                const event = JSON.parse(line.slice(6));
                                displayEvent(event);
                            } catch (e) {
                                console.error('Failed to parse event:', e);
                            }
                        }
                    }
                }
            } catch (error) {
                console.error('Error executing code:', error);
                output.innerHTML += `<div class="event" style="border-color: red;">Error: ${error.message}</div>`;
            } finally {
                executeBtn.disabled = false;
                executeBtn.textContent = 'Execute Code';
            }
        }
        
        function displayEvent(event) {
            const output = document.getElementById('output');
            const eventDiv = document.createElement('div');
            eventDiv.className = 'event';
            
            if (event.type === 'stdout') {
                eventDiv.innerHTML = `<strong>Output:</strong> ${event.text}`;
            } else if (event.type === 'execute_result') {
                eventDiv.innerHTML = `<strong>Result:</strong> ${event.data['text/plain']}`;
            } else if (event.status) {
                eventDiv.innerHTML = `<strong>Status:</strong> ${event.status}`;
                eventDiv.style.borderColor = event.status === 'complete' ? 'green' : '#007acc';
            } else {
                eventDiv.innerHTML = `<pre>${JSON.stringify(event, null, 2)}</pre>`;
            }
            
            output.appendChild(eventDiv);
            output.scrollTop = output.scrollHeight;
        }
        
        function clearOutput() {
            document.getElementById('output').innerHTML = '';
        }
        
        // 페이지 로드 시 자동으로 커널 생성
        window.onload = () => {
            createKernel();
        };
    </script>
</body>
</html>
'''

# 통합 테스트 실행 함수
async def run_all_tests():
    """모든 테스트 실행"""
    print("=== SSE FD Passing Complete Test Suite ===\\n")

    # 기본 사용 예제
    await basic_usage_example()
    await asyncio.sleep(2)

    # 성능 테스트
    print("\\n" + "="*50)
    perf_test = SSEPerformanceTest()
    await perf_test.test_connection_overhead(20)
    await asyncio.sleep(2)

    await perf_test.test_streaming_performance()
    await asyncio.sleep(2)

    # 벤치마크
    print("\\n" + "="*50)
    benchmark = SSEBenchmarkComparison()
    await benchmark.run_comparison()
    await asyncio.sleep(2)

    # Jupyter 시뮬레이션
    print("\\n" + "="*50)
    notebook_sim = JupyterNotebookSimulator()
    await notebook_sim.simulate_notebook_session()
    await asyncio.sleep(2)

    # 모니터링 (짧게)
    print("\\n" + "="*50)
    monitor = SSEMonitoring()
    health = await monitor.health_check()
    print(f"Health check: {'PASS' if health else 'FAIL'}")

    # 부하 테스트 (가벼운 버전)
    print("\\n" + "="*50)
    load_test = SSELoadTest()
    await load_test.concurrent_connections_test(10)

    print("\\n=== All tests completed ===")

if __name__ == "__main__":
    # 배포 파일 생성
    with open("Dockerfile", "w") as f:
        f.write(DOCKERFILE_SSE)

    with open("requirements.txt", "w") as f:
        f.write(REQUIREMENTS_SSE)

    with open("client.html", "w") as f:
        f.write(CLIENT_HTML)

    print("Deployment files created:")
    print("  - Dockerfile")
    print("  - requirements.txt")
    print("  - client.html (웹 클라이언트)")
    print()
    print("Usage:")
    print("  1. python main.py (서버 시작)")
    print("  2. python test_sse_fd_passing.py (테스트 실행)")
    print("  3. Open client.html in browser (웹 클라이언트)")
    print()

    # 설정 출력
    config = SSEProductionConfig.get_recommended_config()
    print("Production Configuration:")
    print(json.dumps(config, indent=2))

    # 테스트 실행 (주석 해제하면 실행)
    # asyncio.run(run_all_tests())