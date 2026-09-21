# 实验室传感器时间线归并 API（FastAPI + SQLite）

多个传感器来源（source）按各自的序列号（seq）**乱序批量上报**带事件时间（`observed_at`）的读数。
服务以“**最慢来源的事件时间 − 允许迟到量**”维护水位线（watermark），只发布越过水位线、
且此前尚未发布的读数，并按 `(observed_at, source_id, seq)` 输出**确定顺序**的归并流。
越过**已发布**水位线才到达的迟到读数不会改写历史，而是进入**隔离区**供查询。

- 语言/框架：Python 3.11 · FastAPI · Uvicorn
- 存储：SQLite（WAL 模式，单文件，零外部依赖）
- 并发：写操作在 `BEGIN IMMEDIATE` 事务内串行化，水位线推进单调、发布只发生一次

## 单条命令启动

```bash
docker compose up --build
```

启动后：

- 服务地址：`http://localhost:8000`
- 交互文档（Swagger UI）：`http://localhost:8000/docs`
- OpenAPI JSON：`http://localhost:8000/openapi.json`
- 健康检查：`http://localhost:8000/health`

数据库文件在容器内 `/data/sensors.db`，由 compose 命名卷 `sensor-data` 持久化；
建库与建表在应用启动时（FastAPI lifespan）自动完成，无需手动初始化。
停止并清理：`docker compose down`（加 `-v` 同时删除数据卷）。

## 必要配置

通过环境变量配置（compose 中已给默认值，可按需覆盖）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SENSOR_HOST` | `0.0.0.0` | uvicorn 绑定地址 |
| `SENSOR_PORT` | `8000` | 监听端口（容器内固定 `EXPOSE 8000`，改宿主机端口改 compose 的 `ports`） |
| `SENSOR_DB_PATH` | `/data/sensors.db` | SQLite 文件路径，父目录不存在会自动创建 |

## 本地开发（不用 Docker）

```bash
python3 -m pip install -r requirements.txt
SENSOR_DB_PATH=./sensors.db uvicorn app.main:app --reload --port 8000
pytest -q        # 运行 22 个行为测试（需 pytest、httpx）
```

## 语义说明

1. **创建会话固定来源列表与迟到容忍**：`source_ids` 之后不可更改；上报未知 `source_id` 返回 400。
2. **首报门槛**：所有来源都至少上报一次后水位线才有定义；此前读数全部缓冲、不发布、不隔离。
3. **水位线**：`WM = min(每个来源已知最大 observed_at) − allowed_lateness_seconds`，单调不减
   （来源时钟回拨不会使已发布水位线倒退）。
4. **发布规则**：仅当 `observed_at < WM`（严格小于，落在边界上的读数继续等待）时发布；
   每次上报只发布此前未发布的缓冲读数。发布顺序固定为 `observed_at, source_id, seq`。
5. **幂等**：重复提交 `(source_id, seq)` 且 `value` 相同 → 计为 `duplicates` 的空操作；
   同 key 不同 payload → 409，且该批次整体回滚（不会半写）。
6. **隔离（quarantine）**：读数到达时若 `observed_at < 已发布水位线`，它永远无法再进入已发布流，
   原样写入隔离区（记录当时的水位线），已发布历史不发生任何变化。
7. 时间统一按 UTC 处理：接受 `...Z`、带偏移量（`+02:00`）或不带时区（按 UTC）的 ISO 8601 字符串。

## 接口

### 1. 创建会话

`POST /sessions`

```bash
curl -s -X POST localhost:8000/sessions -H 'Content-Type: application/json' -d '{
  "session_id": "lab-1",
  "source_ids": ["thermometer-a", "thermometer-b"],
  "allowed_lateness_seconds": 10
}'
```

`session_id` 可省略（自动生成 UUID hex）。响应 201。

### 2. 批量上报读数（乱序、迟到均可）

`POST /sessions/{session_id}/readings`

请求体支持 `{"readings": [...]}`，也接受裸 JSON 数组：

```bash
curl -s -X POST localhost:8000/sessions/lab-1/readings \
  -H 'Content-Type: application/json' -d '{
  "readings": [
    {"source_id": "thermometer-b", "seq": 2, "observed_at": "2026-09-21T10:00:20Z", "value": 21.4},
    {"source_id": "thermometer-a", "seq": 1, "observed_at": "2026-09-21T10:00:05Z", "value": 22.1}
  ]
}'
```

响应汇总本批处理结果：

```json
{
  "session_id": "lab-1",
  "received": 2,
  "duplicates": 0,
  "quarantined": 0,
  "published_now": 0,
  "published_total": 0,
  "buffered": 2,
  "all_sources_reported": true,
  "published_watermark": null
}
```

字段：`received` 批内条数；`duplicates` 幂等重复条数；`quarantined` 本批被隔离条数；
`published_now` 本批触发新发布的条数；`published_total` 会话累计已发布；`buffered` 仍在等待的条数。

错误码：未知来源/非法时间/空批次 → 400；字段结构错误（缺字段、`seq<0` 等）→ 422；
同 key 不同 payload → 409；会话不存在 → 404。

### 3. 拉取已发布流

`GET /sessions/{session_id}/stream?limit=100&cursor=...`

- 只返回已发布读数，严格按 `(observed_at, source_id, seq)` 排序；
- keyset 分页：响应中的 `next_cursor` 原样回传即可翻页，为 `null` 表示取完；
- 每条读数带发布时的 `published_watermark`，响应体也带当前水位线。

```bash
curl -s 'localhost:8000/sessions/lab-1/stream?limit=50'
```

### 4. 查看隔离项

`GET /sessions/{session_id}/quarantine?limit=100&cursor=...`

按到达时间排列的迟到读数，含 `observed_at`、原始 `value` 与被拦截时的
`quarantined_watermark`（`reason` 固定为 `behind_published_watermark`）。同样支持游标分页。

```bash
curl -s 'localhost:8000/sessions/lab-1/quarantine'
```

### 5. 会话状态（辅助接口）

`GET /sessions/{session_id}`：返回来源列表、迟到容忍、是否已全部首报、
已发布水位线与当前计算出的有效水位线。

## 项目结构

```
.
├── app/
│   ├── main.py          # FastAPI 路由、异常映射
│   ├── service.py       # 水位线计算 / 发布 / 隔离 / 幂等等核心逻辑
│   ├── database.py      # SQLite schema、连接、BEGIN IMMEDIATE 事务
│   ├── models.py        # Pydantic 请求/响应模型
│   ├── time_utils.py    # ISO 8601 解析与 UTC 规范化
│   └── config.py        # 环境变量配置
├── tests/test_api.py    # 22 个端到端行为测试
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
