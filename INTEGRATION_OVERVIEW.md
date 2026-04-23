# picapi_service 功能清单与插件对接计划

本文档用于梳理 `picapi_service` 当前能力，并给出与 `astrbot_plugin_pic_rater` 的对接计划。

## 1. picapi_service 当前功能总览

### 1.1 API 能力

- `GET /health`
  - 返回服务可用状态、图库配置、分类数量、文件总数。
- `GET /categories`
  - 返回顶级分类列表。
- `GET /dirs?path=...`
  - 返回目录下子目录和文件统计。
- `GET /random_pic`
  - 随机选图；支持 `cat`、`q`、`bias`、`alpha`、`redirect`。
- `POST /rate`
  - 评分图片，更新统计并可触发图片元数据写回。
- `POST /reindex`
  - 扫描图库重建主表，支持清理失效记录。
- `POST /sync_subjects`
  - 从图片文件元数据同步标签到数据库。
- `GET /search`
  - 检索链路：FAISS 优先，失败回退 FTS/LIKE。
- `GET /admin/sync_progress`
  - 返回标签同步进度。
- `POST /admin/rebuild_fts`
  - 重建 FTS 索引。
- `POST /admin/refresh_fts_tags`
  - 刷新 FTS tags 字段。

### 1.2 存储与架构能力

- 双后端元数据存储：SQLite / PostgreSQL（`MetadataStore` 抽象层）。
- 自动后端选择：`DatabaseManager`（可配置 PG 优先，支持安全回退）。
- 自动迁移：SQLite -> PostgreSQL（批量迁移、断点续跑、dry-run、limit、统计）。
- 检索回填抽象：支持 `ids -> MetadataStore.get_by_ids(ids)` 批量回填，避免 N+1。

### 1.3 检索能力

- 文本检索：SQLite FTS5 + LIKE 回退。
- 向量检索：FAISS 索引加载 + 查询编码（可选开关）。
- 统一回填：FAISS 命中 id 后批量回表得到 `file_path + metadata`。

### 1.4 运维能力

- 健康检查接口。
- 索引重建和标签刷新管理接口。
- 迁移 CLI：`python migrate.py`。
- 可配置运行参数（`.env`）。

## 2. 与 astrbot 插件的现状映射

插件仓库：`D:/pythonProject/astrbot_plugin_pic_rater`

### 2.1 已对接能力

- `#来一张` -> `GET /random_pic`
- `#评分` -> `POST /rate`
- `#图类目` -> `GET /categories` + `GET /dirs`
- `#整理图库` -> `POST /reindex` + `POST /sync_subjects` + `GET /admin/sync_progress`

### 2.2 服务端已有但插件未使用能力

- `GET /search`（含 FAISS 优先策略）
- `GET /health`
- `POST /admin/rebuild_fts`
- `POST /admin/refresh_fts_tags`

## 3. 关键对接风险与注意事项

1. `POST /reindex` 参数形式建议统一  
   插件当前有 bool 与对象两种写法兼容尝试，建议固化为一种。

2. `POST /sync_subjects` 参数约定需明确  
   如需传 `limit`，建议统一 query 方式。

3. `#来一张` 的 q/cat 自动判别可能误判  
   建议支持显式前缀（如 `q:`、`cat:`）优先。

4. 插件会话态是内存态  
   机器人重启后“上一张图”上下文会丢失。

5. 生产环境建议开启安全回退策略  
   `ALLOW_SQLITE_FALLBACK=false` 时 PG 异常应 fail-fast，避免数据分叉。

## 4. 建议对接计划（分阶段）

### Phase A（最小改造，先稳）

- 插件新增 `#服务状态` 命令调用 `/health`。
- 插件固定 `reindex` 请求格式，不再双分支尝试。
- 明确 `sync_subjects` 参数传法和默认行为。

交付结果：
- 可观测性更好，联调与排障成本明显下降。

### Phase B（检索能力上屏）

- 插件新增 `#搜图 <关键词或路径>` 命令。
- 直接调用 `GET /search`，优先消费 `mode=faiss_metadata` 的结果。
- 若回退到 `fts_or_like`，插件提示“当前为文本检索结果”。

交付结果：
- 用户在聊天侧可直接使用向量检索能力，无需改 FAISS 核心逻辑。

### Phase C（运维与质量增强）

- 插件新增运维命令：
  - `#重建检索索引` -> `/admin/rebuild_fts`
  - `#刷新标签索引` -> `/admin/refresh_fts_tags`
- 增加失败重试和友好错误文案（网络超时、后端不可用等）。

交付结果：
- 索引维护可以在聊天场景下闭环。

## 5. 推荐联调清单

1. `#来一张`（q/cat 两类输入）
2. `#评分`（正常、非法分值、找不到图片）
3. `#图类目`（顶级与子目录）
4. `#整理图库`（含进度显示）
5. `#搜图`（命中 / 为空 / 回退）
6. `#服务状态`

## 6. 下一步建议

优先实施 Phase A + Phase B。  
这样可以在保持当前稳定性的前提下，把你已经接好的 FAISS+MetadataStore 检索链路真正暴露给插件用户。
