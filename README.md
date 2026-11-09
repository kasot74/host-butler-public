# host-butler

一個 Claude API 的共用通道 — 讓你用自己的工具（`claude` CLI、Anthropic SDK、或任何能打 Anthropic Messages API 的 client）呼叫 Claude，不需要自己申請 Anthropic 帳號或 API key。真正的金鑰留在服務端，你只拿到一把專屬的「虛擬金鑰」。

## 需要環境

依你要用的方式，先裝好對應的工具：

- **`claude` CLI**：需要 Node.js（建議 18 以上）與 npm，再安裝 CLI：
  ```bash
  npm install -g @anthropic-ai/claude-code
  ```
- **`curl`**：多數系統已內建，不用額外安裝。
- **Python SDK**：
  ```bash
  pip install anthropic
  ```
- **TypeScript / Node.js SDK**：需要 Node.js 與 npm：
  ```bash
  npm install @anthropic-ai/sdk
  ```

## 怎麼接上

只要設定兩個環境變數，指向這個通道的對外端點：

```bash
export ANTHROPIC_BASE_URL=https://<gateway-domain>/butler
export ANTHROPIC_API_KEY=bk-xxxxxxxx   # 管理者給你的虛擬金鑰
```

### 用 `claude` CLI

```bash
ANTHROPIC_BASE_URL=https://<gateway-domain>/butler \
ANTHROPIC_API_KEY=bk-xxxxxxxx \
claude -p "hi"
```

或寫進 `~/.claude/settings.json` 的 `env` 讓它永久生效：

```json
"env": {
  "ANTHROPIC_BASE_URL": "https://<gateway-domain>/butler",
  "ANTHROPIC_API_KEY": "bk-xxxxxxxx"
}
```

### 用 `curl`

```bash
curl https://<gateway-domain>/butler/v1/messages \
  -H "x-api-key: bk-xxxxxxxx" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{
    "model": "claude-sonnet-4-5",
    "max_tokens": 1024,
    "messages": [{"role": "user", "content": "hi"}]
  }'
```

### 用 Anthropic SDK（Python / TypeScript 等）

把 SDK 的 base URL 跟 API key 指過來即可，其餘用法與官方文件完全相同：

```python
import anthropic

client = anthropic.Anthropic(
    base_url="https://<gateway-domain>/butler",
    api_key="bk-xxxxxxxx",
)
```

```typescript
import Anthropic from "@anthropic-ai/sdk";

const client = new Anthropic({
  baseURL: "https://<gateway-domain>/butler",
  apiKey: "bk-xxxxxxxx",
});
```

虛擬金鑰也可以放在 `Authorization: Bearer bk-xxxxxxxx`，兩種帶法都接受。

### 用 Docker（`.env`）

如果你的程式是跑在 Docker 容器裡，一樣把這兩個環境變數傳進容器即可。

新增一個 `.env` 檔：

```
ANTHROPIC_BASE_URL=https://<gateway-domain>/butler
ANTHROPIC_API_KEY=bk-xxxxxxxx
```

`docker-compose.yml` 用 `env_file` 帶入：

```yaml
services:
  app:
    image: your-app-image
    env_file:
      - .env
```

或單純用 `docker run --env-file`：

```bash
docker run --env-file .env your-app-image
```

容器裡的程式（`claude` CLI、curl、Anthropic SDK）照常讀這兩個環境變數，不需要再改任何程式碼。

## 自己架這個 Gateway 需要準備什麼

以上是「使用」這條通道的方式。如果你是要自己部署、架一份 host-butler，需要先準備：

- **一台能跑 Docker 的主機**，裝好 Docker 與 `docker compose`（v2）。
- **一個 Anthropic API key**（`sk-ant-...`），或一份 `claude` CLI 的訂閱登入憑證 — 作為這條通道實際轉發時用的上游驗證，放進 `.env` 的 `ANTHROPIC_API_KEY`。
- **對外網域 + TLS**：公開服務需要一個網域與憑證（例如 Let's Encrypt），搭配反向代理（如 nginx）把 `/butler/` 轉到容器的 `LISTEN_PORT`（預設 `8788`）；只在本機用則可省略,直接打 loopback。
- **自己發的虛擬金鑰**：決定要發給誰、幾把，寫進 `.env` 的 `GW_TOKENS`（逗號分隔多把）；產生方式：
  ```bash
  python3 -c "import secrets;print('bk-'+secrets.token_hex(24))"
  ```
- **設定檔**：複製 `.env.example` 成 `.env`，依需求填上面兩項與其他變數。

準備好以上,啟動方式：

```bash
cp .env.example .env   # 填好金鑰與虛擬金鑰
docker compose up -d
```
