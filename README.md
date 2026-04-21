# picapi_service

独立图片服务（FastAPI + SQLite + ExifTool），供 `astrbot_plugin_pic_rater` 插件通过 HTTP 调用。

## Docker 部署

参考 `docker-compose.example` 与 `.env.example`：

- 将 `docker-compose.example` 复制为你的 `docker-compose.yml`
- 按需修改挂载目录（图库目录到容器内 `/data/gallery`）
- 启动：
#首次部署需要创建网络
#docker network create astrbot_network
```bash
docker compose up -d --build
```

## 健康检查

- `GET /health`

## 主要接口

- `GET /random_pic`：随机取图（支持 `q` / `cat` / `bias` / `alpha`）
- `POST /rate`：写入评分并按阈值回写 XMP
- `POST /reindex`：扫描入库
- `POST /sync_subjects`：同步 XMP:Subject 标签
- `GET /search`：检索（FTS 优先，失败回退 LIKE）

## 运行依赖

- 容器内已安装 `exiftool`
- SQLite DB 默认在 `/data/db/picapi.sqlite`

