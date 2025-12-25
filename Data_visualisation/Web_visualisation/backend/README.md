# Novel Data Insight Dashboard - Backend

## 运行说明

### 1. 安装依赖

确保已安装 Python 3.10+。

```bash
pip install -r requirements.txt
```

### 2. 配置数据库 (可选)

默认使用 SQLite (`novel_data.db`)，开箱即用。
如需使用 MySQL，请修改 `app/core/config.py` 中的 `DATABASE_URL`，或设置环境变量 `DATABASE_URL`。

### 3. 启动服务

```bash
python app/main.py
```
或者
```bash
uvicorn backend.main:app --reload
```

### 4. 接口文档

启动后访问: [http://localhost:8000/docs](http://localhost:8000/docs)

### 注意事项
- 系统启动时会自动检测数据库，若为空则自动注入模拟数据。
- 包含了所有需求文档中要求的 API 接口。
