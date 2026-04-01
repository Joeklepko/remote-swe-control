import asyncio
import subprocess
import uuid
import os
import signal
from datetime import datetime
from typing import List, Dict, Optional

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# 基础配置
MAX_LOG_HISTORY = 5000
MAX_COMPLETED_HISTORY = 500
DEFAULT_TIMEOUT = 3600  # 1小时超时

# 全局状态
task_queue = asyncio.Queue()
current_task = None
waiting_tasks = []
completed_tasks = []
log_history = []
connected_websockets = []
# 确保 active_process 在全局可用，以便进行强制停止操作
active_process: Optional[asyncio.subprocess.Process] = None

class JobRequest(BaseModel):
    instance_id: str
    resolved_path: str = "./resolved_path"
    cleanup_images: bool = True
    timeout: int = DEFAULT_TIMEOUT

async def broadcast_log(message: str):
    """向所有连接的 WebSocket 广播日志并存入历史"""
    log_history.append(message)
    if len(log_history) > MAX_LOG_HISTORY:
        log_history.pop(0)
    for ws in connected_websockets:
        try:
            await ws.send_text(message)
        except:
            pass

async def worker():
    """核心后台任务处理协程"""
    global current_task, waiting_tasks, completed_tasks, active_process
    while True:
        # 从队列中获取任务
        job = await task_queue.get()
        current_task = job
        
        # 提取参数
        instance_id = job['request'].instance_id
        work_path = job['request'].resolved_path
        
        # --- 核心逻辑修改：增量跳过检查 ---
        # 预检：如果 patch 文件已存在，则无需重新运行
        patch_file_path = os.path.join(work_path, f"{instance_id}.diff")
        
        if os.path.exists(patch_file_path):
            await broadcast_log(f">>> [SYSTEM] 检测到已存在补丁文件: {patch_file_path}")
            await broadcast_log(f">>> [SYSTEM] 自动跳过任务并标记为完成: {instance_id}")
            
            job['status'] = 'success'
            job['start_time'] = datetime.now().strftime("%H:%M:%S")
            job['end_time'] = datetime.now().strftime("%H:%M:%S")
            job['exit_code'] = 0
            
            # 更新状态列表
            waiting_tasks = [t for t in waiting_tasks if t['id'] != job['id']]
            completed_tasks.insert(0, job)
            
            # 重置当前任务状态并继续下一个
            current_task = None
            task_queue.task_done()
            continue
        # --- 增量检查结束 ---

        # 正常执行流程
        job['start_time'] = datetime.now().strftime("%H:%M:%S")
        job['status'] = 'running'
        waiting_tasks = [t for t in waiting_tasks if t['id'] != job['id']]
        
        await broadcast_log(f">>> [SYSTEM] 任务启动: {instance_id}")

        # 构建执行命令
        cmd = [
            "python3", "relay_agent_runner.py", 
            "--instance_id", instance_id,
            "--resolved_path", work_path
        ]
        if job['request'].cleanup_images:
            cmd.append("--cleanup_images")

        try:
            # 创建子进程，并开启进程组 (preexec_fn)
            active_process = await asyncio.create_subprocess_exec(
                *cmd, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.STDOUT,
                preexec_fn=os.setsid 
            )

            async def read_stream():
                while True:
                    line = await active_process.stdout.readline()
                    if not line:
                        break
                    await broadcast_log(line.decode().strip())

            # 等待任务读取流完成或超时
            await asyncio.wait_for(read_stream(), timeout=job['request'].timeout)
            exit_code = await active_process.wait()
            
            if job['status'] != 'stopped':
                job['status'] = 'success' if exit_code == 0 else 'failed'
                job['exit_code'] = exit_code

        except asyncio.TimeoutError:
            try:
                # 超时处理：杀死整个进程组
                os.killpg(os.getpgid(active_process.pid), signal.SIGKILL)
                await broadcast_log(f">>> [ERROR] 任务超时 ({job['request'].timeout}s)，进程组已清理")
            except:
                pass
            job['status'] = 'timeout'
            job['exit_code'] = -1
        
        except Exception as e:
            if job['status'] != 'stopped':
                job['status'] = 'failed'
                await broadcast_log(f">>> [SYSTEM] 任务运行异常: {str(e)}")

        # 任务收尾
        job['end_time'] = datetime.now().strftime("%H:%M:%S")
        completed_tasks.insert(0, job)
        if len(completed_tasks) > MAX_COMPLETED_HISTORY:
            completed_tasks.pop()
        
        current_task = None
        active_process = None
        task_queue.task_done()

@app.on_event("startup")
async def startup():
    asyncio.create_task(worker())

@app.post("/api/submit")
async def submit(request: JobRequest):
    job = {"id": str(uuid.uuid4())[:8], "request": request, "status": "pending"}
    waiting_tasks.append(job)
    await task_queue.put(job)
    return {"status": "queued"}

@app.post("/api/stop")
async def stop_current_job():
    global active_process, current_task, completed_tasks
    if active_process and current_task:
        try:
            job = current_task
            job['status'] = 'stopped'
            job['exit_code'] = -9
            job['end_time'] = datetime.now().strftime("%H:%M:%S")
            
            # 强力杀死进程组及其所有子进程
            pid = active_process.pid
            try:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            except:
                active_process.kill()
            
            completed_tasks.insert(0, job)
            if len(completed_tasks) > MAX_COMPLETED_HISTORY:
                completed_tasks.pop()
            
            current_task = None
            active_process = None
            
            await broadcast_log(">>> [SYSTEM] 收到停止指令：当前进程组已强制清理")
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    return {"status": "error", "message": "No active job running"}

@app.post("/api/retry/{job_id}")
async def retry_job(job_id: str):
    target_job = next((h for h in completed_tasks if h['id'] == job_id), None)
    if target_job:
        new_job = {
            "id": str(uuid.uuid4())[:8], 
            "request": target_job['request'], 
            "status": "pending"
        }
        waiting_tasks.append(new_job)
        await task_queue.put(new_job)
        return {"status": "retrying"}
    return {"status": "error", "message": "Job not found"}

@app.get("/api/status")
async def get_status():
    return {
        "current_task": current_task, 
        "waiting_tasks": waiting_tasks, 
        "completed_tasks": completed_tasks
    }

@app.post("/api/clear_logs")
async def clear_logs():
    global log_history
    log_history = []
    return {"ok": True}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_websockets.append(websocket)
    # 连接时发送历史日志
    if log_history:
        for line in log_history:
            await websocket.send_text(line)
    try:
        while True:
            await websocket.receive_text()
    except:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)

@app.get("/", response_class=HTMLResponse)
async def index():
    if os.path.exists("index.html"):
        with open("index.html", "r") as f:
            return f.read()
    return "index.html not found"

if __name__ == "__main__":
    import uvicorn
    # 运行在 8000 端口
    uvicorn.run(app, host="0.0.0.0", port=8000)
