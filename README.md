# FastAPI 本地运行

## 环境要求
- Python 3.11+
- Windows PowerShell 7（或其他终端）

## 安装依赖
在项目根目录或 `fastapi` 目录打开终端，执行：

```powershell
cd fastapi
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -e .
```

> 如果你更习惯使用 `uv`：
> ```powershell
> cd fastapi
> uv pip install -e .
> ```

## 启动开发服务
方式一：用 `python` 直接运行（已在 `main.py` 内置）
```powershell
cd fastapi
.\.venv\Scripts\Activate.ps1
python main.py
```

方式二：使用 uvicorn CLI
```powershell
cd fastapi
.\.venv\Scripts\Activate.ps1
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

启动后访问：
- 健康检查: `http://127.0.0.1:8000/health`
- 根路径: `http://127.0.0.1:8000/`

## CORS 设置
后端默认允许本地前端（Vite）地址：
- `http://localhost:5173`
- `http://127.0.0.1:5173`

如需自定义，可设置环境变量 `ALLOWED_ORIGINS`（逗号分隔）：
```powershell
$env:ALLOWED_ORIGINS="http://localhost:5173,http://127.0.0.1:5173"
python main.py
```

## 常见问题
- 端口已占用：修改 `PORT` 环境变量或命令行的 `--port`。
- 本地前端跨域：确认前端地址是否在允许列表中，或配置 `ALLOWED_ORIGINS`。

