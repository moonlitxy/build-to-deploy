# DeployOne 一键上线 · 本地构建发布平台

专为“**开发者本地可连接服务器 VPN 与 Git 独立 VPN，但远程服务器无法访问 Git、无法安装使用 Jenkins**”场景量身打造。

---

## 解决的 5 大核心需求

1. **本地运行，无需登录**：
   - 开发者直接在自己电脑运行一个可执行文件，访问 `http://localhost:8080`。
   - **完全去除繁琐的登录认证**，本地打开网页直接操作。
2. **服务器独立目录隔离与自定义构建命令**：
   - **产物目录 (ArtifactDir)**：存放本地上传的 `jar` 包或 `go` 二进制文件（如 `/data/packages/order-service`）。
   - **镜像构建目录 (BuildDir)**：存放 Dockerfile 与构建上下文（如 `/data/docker-build/order-service`）。
   - **K8s 配置目录 (K8sYamlDir)**：存放 Deployment / Service 等配置（如 `/data/k8s-yaml/order-service`）。
   - **版本控制与构建命令**：构建命令模板化（如 `cd {{.BuildDir}} && docker build -t {{.Image}}:{{.Tag}} .`），镜像 Tag 与版本严格由用户控制。
3. **版本冲突预检与弹窗确认（强制覆盖保护）**：
   - 开始发布前，系统自动在目标服务器预检：如果远程 Docker 缓存或 K8s 线上 Pod 中已存在完全相同的 Tag，**立刻阻断并弹出告警确认框**。
   - 只有用户点击“确认覆盖并继续发布”后，才允许以 `force=true` 覆盖发布，防止误覆盖线上版本。
4. **K8s 命名空间与 Pod 状态实时监控**：
   - 提供专属的 **☸️ K8s Pod 监控** 模块。
   - 支持动态选择命名空间（Namespace），实时列出所有 Pod 的状态、就绪数（如 1/1）、重启次数、运行时长、Pod IP、节点与镜像。
   - 支持一键**查看容器日志**（实时获取最新 200 行）。
5. **SSH 长连接保活心跳池（防频繁登录限制）**：
   - 后台内置 SSH/SFTP 客户端长连接池与每 25 秒探测保活心跳。
   - 仅在首次或网络断开时握手，之后所有上传、命令执行与状态查询**完全复用长连接**，杜绝频繁 SSH 登录触发服务器安全限制或封禁。
   - 支持在网页上一键“⚡ 测试长连接”。

---

## 快速使用

### 1. 本地极简启动（Mac / Linux / Windows）

在项目根目录下直接运行（自动探测并加载配置文件）：
```bash
make dev            # 首次运行会基于 config.example.yaml 生成开发配置 configs/config.dev.yaml，随后启动
make linux          # 交叉编译 Linux 部署包到 bin/
make windows        # 交叉编译 Windows 可执行文件
```

没有配置时会按 `configs/config.dev.yaml` → `configs/config.yaml` → 根目录 `config.yaml` 的顺序自动探测（开发配置优先）；生产环境建议用 `-config configs/config.yaml` 显式指定。首次部署把 `configs/config.example.yaml` 复制为 `configs/config.yaml` 后按注释修改即可。

打开本地浏览器访问：
👉 **http://localhost:8080** (无需输入任何登录密码)

### 2. 发布操作流程

1. **产物发布**：
   - 选择应用（自动显示其绑定的服务器、产物目录、构建目录、K8s 目录）。
   - 拖拽或选择本地修改构建好的 `jar` 或 `go` 二进制文件。
   - 输入版本 Tag（若已存在将弹出覆盖确认）。
   - 点击“开始上传并构建部署”：
     - `分片上传` → `SFTP 传至产物目录` → `同步至构建目录` → `执行镜像构建` → `执行 K8s 部署` → `等待 Pod 滚动就绪`。
     - 控制台实时显示彩色日志（SSE 流式推送）。
2. **K8s Pod 状态查看**：
   - 点击顶部 “☸️ K8s Pod 监控” 标签。
   - 切换命名空间，实时查看 Pod 是否已完成更新就绪，并可一键调取日志排查问题。
3. **修改目录与服务器**：
   - 点击顶部 “⚙️ 服务器与目录配置” 标签，直接在网页上维护服务器 SSH 信息与各个应用的独立目录。
