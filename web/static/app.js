'use strict';

const $ = (id) => document.getElementById(id);
const CHUNK_SIZE = 4 * 1024 * 1024;
const TAG_RE = /^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$/;

const state = {
  apps: [],
  servers: [],
  selectedFile: null,
  uploadId: null,
  releaseId: null,
  eventSource: null,
  isDeploying: false,
  pendingReleaseParams: null,
};

// ---------------- 基础工具函数 ----------------
async function api(method, url, body) {
  const opt = { method, headers: {} };
  if (body !== undefined) {
    opt.headers['Content-Type'] = 'application/json';
    opt.body = JSON.stringify(body);
  }
  const res = await fetch(url, opt);
  let data = null;
  try {
    data = await res.json();
  } catch (_) {}
  return { ok: res.ok, status: res.status, data: data || {} };
}

function fmtSize(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  while (bytes >= 1024 && i < units.length - 1) {
    bytes /= 1024;
    i++;
  }
  return bytes.toFixed(i === 0 ? 0 : 2) + ' ' + units[i];
}

function fmtTime(dateStr) {
  if (!dateStr) return '-';
  const d = new Date(dateStr);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function escapeHtml(str) {
  return String(str ?? '').replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
}

// ---------------- 初始化 ----------------
document.addEventListener('DOMContentLoaded', async () => {
  setupTabs();
  setupDropZone();
  setupButtons();
  await refreshConfigs();
});

function setupTabs() {
  document.querySelectorAll('.tab-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
      document.querySelectorAll('.tab-pane').forEach((p) => p.classList.remove('active'));
      btn.classList.add('active');
      const tabId = 'tab-' + btn.dataset.tab;
      $(tabId).classList.add('active');

      if (btn.dataset.tab === 'pods') {
        loadNamespacesAndPods();
      } else if (btn.dataset.tab === 'history') {
        loadHistory();
      }
    });
  });
}

async function refreshConfigs() {
  const [appsRes, srvRes] = await Promise.all([
    api('GET', '/api/apps'),
    api('GET', '/api/servers'),
  ]);

  state.apps = appsRes.data || [];
  state.servers = srvRes.data || [];

  renderAppSelects();
  renderServerCards();
  renderAppCards();
  updatePathPreview();
}

function renderAppSelects() {
  const opts = state.apps.map((a) => `<option value="${a.id}">${escapeHtml(a.name)} (${escapeHtml(a.type)})</option>`).join('');
  $('deployAppSelect').innerHTML = opts || '<option value="">暂无应用，请先在配置中添加</option>';
  $('historyAppFilter').innerHTML = '<option value="">全部应用</option>' + opts;

  // Pod 页面服务器下拉
  const srvOpts = state.servers.map((s) => `<option value="${s.id}">${escapeHtml(s.name)} (${escapeHtml(s.host)})</option>`).join('');
  $('podServerSelect').innerHTML = srvOpts || '<option value="">暂无服务器</option>';
}

function updatePathPreview() {
  const appId = $('deployAppSelect').value;
  const app = state.apps.find((a) => a.id === appId);
  if (!app) {
    $('appPathPreview').classList.add('hidden');
    return;
  }
  $('appPathPreview').classList.remove('hidden');
  const srv = state.servers.find((s) => s.id === app.serverId);
  $('pvServer').textContent = srv ? `${srv.name} (${srv.user}@${srv.host})` : (app.serverId || '-');
  $('pvArtifactDir').textContent = app.artifactDir || '未配置';
  $('pvBuildDir').textContent = app.buildDir || '未配置';
  const yamlPath = app.k8sYamlFile ? (app.k8sYamlDir ? `${app.k8sYamlDir}/${app.k8sYamlFile}` : app.k8sYamlFile) : (app.k8sYamlDir || '未配置');
  $('pvYamlDir').textContent = yamlPath;
  $('pvImage').textContent = app.image || '未配置';
}

$('deployAppSelect').addEventListener('change', updatePathPreview);

// ---------------- 本地文件拖拽与选择 ----------------
function setupDropZone() {
  const dropZone = $('dropZone');
  const fileInput = $('fileInput');

  dropZone.addEventListener('click', () => fileInput.click());
  fileInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
      handleSelectedFile(e.target.files[0]);
    }
  });

  dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('dragover');
  });
  dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
  dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    if (e.dataTransfer.files.length > 0) {
      handleSelectedFile(e.dataTransfer.files[0]);
    }
  });
}

function handleSelectedFile(file) {
  state.selectedFile = file;
  const dropZone = $('dropZone');
  dropZone.classList.add('has-file');
  $('dropText').textContent = `已选择: ${file.name} (${fmtSize(file.size)})`;

  // 自动从文件名提取 tag (例如 order-service-1.2.3.jar -> v1.2.3)
  if (!$('tagInput').value.trim()) {
    const match = file.name.match(/(\d+\.\d+(?:\.\d+)?)/);
    if (match) {
      $('tagInput').value = 'v' + match[1];
    }
  }
}

// ---------------- 发布主流程 ----------------
function setupButtons() {
  $('startDeployBtn').addEventListener('click', onStartDeployClick);
  $('conflictConfirmBtn').addEventListener('click', onConfirmConflictDeploy);
  $('conflictCancelBtn').addEventListener('click', () => $('conflictModal').classList.add('hidden'));
  $('clearLogBtn').addEventListener('click', () => { $('logConsole').textContent = ''; });
  $('refreshPodsBtn').addEventListener('click', loadPods);
  $('podServerSelect').addEventListener('change', loadNamespacesAndPods);
  $('podNamespaceSelect').addEventListener('change', loadPods);
  $('podSearchInput').addEventListener('input', filterPodsTable);
  $('closePodLogBtn').addEventListener('click', () => $('podLogModal').classList.add('hidden'));

  // 配置相关模态框事件
  $('addServerBtn').addEventListener('click', () => openServerModal());
  $('closeServerModalBtn').addEventListener('click', () => $('serverModal').classList.add('hidden'));
  $('saveServerBtn').addEventListener('click', saveServer);
  $('testServerConnBtn').addEventListener('click', testServerConnection);
  $('editServerAuthType').addEventListener('change', (e) => {
    $('pwdGroup').classList.toggle('hidden', e.target.value === 'key');
    $('keyGroup').classList.toggle('hidden', e.target.value !== 'key');
  });

  $('addAppBtn').addEventListener('click', () => openAppModal());
  $('closeAppModalBtn').addEventListener('click', () => $('appModal').classList.add('hidden'));
  $('saveAppBtn').addEventListener('click', saveApp);
}

async function onStartDeployClick() {
  if (state.isDeploying) return;
  const appId = $('deployAppSelect').value;
  const tag = $('tagInput').value.trim();
  const remark = $('remarkInput').value.trim();

  if (!appId) return alert('请先选择要发布的应用');
  if (!state.selectedFile) return alert('请先选择本地构建产物 (Jar 或 二进制文件)');
  if (!tag || !TAG_RE.test(tag)) return alert('请输入合法的镜像 Tag (只能包含字母、数字、点、减号、下划线)');

  appendLog(`\n[本地] 准备发布应用: ${appId}, Tag: ${tag}`);
  appendLog('[本地] 正在向服务器执行版本预检 (Precheck)...');

  // 1. 版本冲突预检 (需求 2 核心要求)
  const preRes = await api('POST', '/api/precheck', { appId, tag });
  if (!preRes.ok) {
    appendLog(`[预检失败] ${preRes.data.error || '检查失败'}`);
    return alert(`预检失败: ${preRes.data.error || '未知错误'}`);
  }

  const conflicts = preRes.data.conflicts || [];
  if (conflicts.length > 0) {
    // 发现同版本 Tag，弹出确认弹窗
    state.pendingReleaseParams = { appId, tag, remark };
    showConflictModal(conflicts, tag);
    return;
  }

  // 无冲突，直接开始
  startDeployPipeline(appId, tag, remark, false);
}

function showConflictModal(conflicts, tag) {
  const itemsHtml = conflicts.map((c) => `<div class="conflict-item">⚠️ [${c.source}] ${escapeHtml(c.message)}</div>`).join('');
  $('conflictItems').innerHTML = itemsHtml;
  $('conflictModal').classList.remove('hidden');
}

async function onConfirmConflictDeploy() {
  $('conflictModal').classList.add('hidden');
  if (!state.pendingReleaseParams) return;
  const { appId, tag, remark } = state.pendingReleaseParams;
  appendLog(`[确认覆盖] 用户确认强制覆盖同名镜像版本: ${tag}`);
  startDeployPipeline(appId, tag, remark, true);
}

async function startDeployPipeline(appId, tag, remark, force) {
  state.isDeploying = true;
  $('startDeployBtn').disabled = true;
  updateStatus('running', '正在上传产物...');

  try {
    // 2. 本地文件分片上传
    const upId = await uploadFileChunks(state.selectedFile);
    state.uploadId = upId;

    // 3. 发起发布任务
    appendLog('[本地] 产物已上传至暂存区，触发服务器构建部署流水线...');
    const startRes = await api('POST', '/api/releases', {
      appId,
      tag,
      uploadId: upId,
      force,
      remark,
    });

    if (!startRes.ok) {
      throw new Error(startRes.data.error || '启动发布流水线失败');
    }

    state.releaseId = startRes.data.id;
    appendLog(`[成功] 流水线已启动，任务 ID: ${state.releaseId}`);

    // 4. 监听 SSE 实时进度与日志
    listenReleaseEvents(state.releaseId);
  } catch (err) {
    appendLog(`[错误] ${err.message}`);
    updateStatus('failed', '发布失败');
    state.isDeploying = false;
    $('startDeployBtn').disabled = false;
  }
}

// ---------------- 分片上传 ----------------
async function uploadFileChunks(file) {
  $('uploadProgressContainer').classList.remove('hidden');
  $('uploadProgressBar').style.width = '0%';

  const createRes = await api('POST', '/api/uploads', {
    fileName: file.name,
    size: file.size,
  });
  if (!createRes.ok) throw new Error(createRes.data.error || '初始化上传任务失败');

  const uploadId = createRes.data.id;
  let offset = 0;
  const startTime = Date.now();

  while (offset < file.size) {
    const chunk = file.slice(offset, offset + CHUNK_SIZE);
    const chunkRes = await fetch(`/api/uploads/${uploadId}?offset=${offset}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: chunk,
    });

    if (!chunkRes.ok) {
      const errJson = await chunkRes.json().catch(() => ({}));
      throw new Error(errJson.error || `分片上传失败 HTTP ${chunkRes.status}`);
    }

    const resData = await chunkRes.json();
    offset = resData.received;

    const pct = Math.min(100, Math.round((offset / file.size) * 100));
    $('uploadProgressBar').style.width = pct + '%';
    const elapsedSec = (Date.now() - startTime) / 1000;
    const speed = elapsedSec > 0 ? (offset / elapsedSec) : 0;
    $('uploadProgressInfo').textContent = `${pct}% (${fmtSize(offset)} / ${fmtSize(file.size)})`;
    $('uploadSpeedInfo').textContent = `${fmtSize(speed)}/s`;
  }

  const completeRes = await api('POST', `/api/uploads/${uploadId}/complete`);
  if (!completeRes.ok) throw new Error(completeRes.data.error || '完成上传校验失败');

  appendLog(`✔ 本地分片上传完毕，SHA256: ${completeRes.data.sha256}`);
  return uploadId;
}

// ---------------- SSE 实时日志与阶段跟踪 ----------------
function listenReleaseEvents(releaseId) {
  if (state.eventSource) {
    state.eventSource.close();
  }

  const es = new EventSource(`/api/releases/${releaseId}/events`);
  state.eventSource = es;

  es.addEventListener('release', (e) => {
    const rel = JSON.parse(e.data);
    renderStages(rel.stages || []);
    if (rel.status === 'success') {
      updateStatus('success', '发布完成');
      state.isDeploying = false;
      $('startDeployBtn').disabled = false;
    } else if (rel.status === 'failed' || rel.status === 'canceled') {
      updateStatus('failed', rel.status === 'canceled' ? '已中止' : '发布失败');
      state.isDeploying = false;
      $('startDeployBtn').disabled = false;
    }
  });

  es.addEventListener('log', (e) => {
    const line = JSON.parse(e.data);
    appendLog(line);
  });

  es.addEventListener('end', () => {
    es.close();
    state.isDeploying = false;
    $('startDeployBtn').disabled = false;
    $('uploadProgressContainer').classList.add('hidden');
  });

  es.onerror = () => {
    es.close();
  };
}

function renderStages(stages) {
  if (!stages || stages.length === 0) return;
  const html = stages.map((s) => {
    const icon = s.status === 'success' ? '✔' : s.status === 'failed' ? '✖' : s.status === 'running' ? '⏳' : '○';
    return `<div class="stage-chip ${s.status}">
      <span>${icon}</span>
      <span>${escapeHtml(s.title)}</span>
    </div>`;
  }).join('');
  $('pipelineStages').innerHTML = html;
}

function appendLog(line) {
  const consoleEl = $('logConsole');
  const div = document.createElement('div');
  div.innerHTML = colorizeLine(line);
  consoleEl.appendChild(div);
  if ($('autoScrollLog').checked) {
    consoleEl.scrollTop = consoleEl.scrollHeight;
  }
}

// 日志语法高亮染色 (解决字体全白看不清问题)
function colorizeLine(line) {
  const esc = escapeHtml(line);
  if (esc.startsWith('==&gt;') || esc.startsWith('==>')) {
    return `<span class="log-stage">${esc}</span>`;
  }
  if (esc.startsWith('$ ')) {
    return `<span class="log-cmd">${esc}</span>`;
  }
  if (esc.startsWith('[错误]') || esc.startsWith('[失败]')) {
    return `<span class="log-err">${esc}</span>`;
  }
  if (esc.startsWith('[警告]') || esc.startsWith('⚠️')) {
    return `<span class="log-warn">${esc}</span>`;
  }
  if (esc.startsWith('✔') || esc.includes('成功')) {
    return `<span class="log-ok">${esc}</span>`;
  }

  let colored = esc;
  colored = colored.replace(/^(\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)/, '<span class="log-ts">$1</span>');
  colored = colored.replace(/\b(INFO|Info)\b/g, '<span class="log-info">$1</span>');
  colored = colored.replace(/\b(WARN|WARNING|Warn|Warning)\b/g, '<span class="log-warn">$1</span>');
  colored = colored.replace(/\b(ERROR|FATAL|Error|Fatal|Exception|CrashLoopBackOff|Failed)\b/g, '<span class="log-err">$1</span>');
  colored = colored.replace(/\b(DEBUG|Debug)\b/g, '<span class="log-debug">$1</span>');
  return `<span class="log-line">${colored}</span>`;
}

function colorizeLog(rawText) {
  if (!rawText) return '<span class="log-debug">（无日志输出）</span>';
  return rawText.split('\n').map(colorizeLine).join('');
}

function updateStatus(status, text) {
  const badge = $('currentStatusBadge');
  badge.className = `status-pill status-${status}`;
  badge.textContent = text;
}

// ---------------- K8s Pod 监控 (需求 3) ----------------
let currentPods = [];

async function loadNamespacesAndPods() {
  const srvId = $('podServerSelect').value;
  if (!srvId) return;

  $('podLoading').classList.remove('hidden');
  const nsRes = await api('GET', `/api/k8s/namespaces?serverId=${encodeURIComponent(srvId)}`);
  $('podLoading').classList.add('hidden');

  if (nsRes.ok && nsRes.data) {
    const opts = nsRes.data.map((ns) => `<option value="${escapeHtml(ns)}">${escapeHtml(ns)}</option>`).join('');
    $('podNamespaceSelect').innerHTML = opts || '<option value="default">default</option>';
  }
  loadPods();
}

async function loadPods() {
  const srvId = $('podServerSelect').value;
  const ns = $('podNamespaceSelect').value;
  if (!srvId) return;

  $('podLoading').classList.remove('hidden');
  $('podsTableBody').innerHTML = '<tr><td colspan="9" class="td-empty">正在加载 Pod 状态...</td></tr>';

  const res = await api('GET', `/api/k8s/pods?serverId=${encodeURIComponent(srvId)}&namespace=${encodeURIComponent(ns)}`);
  $('podLoading').classList.add('hidden');

  if (!res.ok) {
    $('podsTableBody').innerHTML = `<tr><td colspan="9" class="td-empty" style="color:var(--danger)">加载失败: ${escapeHtml(res.data.error || '未知错误')}</td></tr>`;
    return;
  }

  currentPods = res.data || [];
  renderPodsTable(currentPods);
}

function renderPodsTable(pods) {
  if (pods.length === 0) {
    $('podsTableBody').innerHTML = '<tr><td colspan="9" class="td-empty">当前命名空间下无 Pod 运行</td></tr>';
    return;
  }

  const rows = pods.map((p) => {
    const isRunning = p.status === 'Running';
    const isPending = p.status.includes('Pending') || p.status.includes('Creating');
    const badgeCls = isRunning ? 'pod-status-running' : isPending ? 'pod-status-pending' : 'pod-status-error';

    return `<tr>
      <td><strong>${escapeHtml(p.name)}</strong></td>
      <td><span class="pod-status-badge ${badgeCls}">${escapeHtml(p.status)}</span></td>
      <td>${escapeHtml(p.ready)}</td>
      <td>${p.restarts}</td>
      <td>${escapeHtml(p.age)}</td>
      <td><code>${escapeHtml(p.ip || '-')}</code></td>
      <td>${escapeHtml(p.node || '-')}</td>
      <td><div style="font-size:11px;max-width:280px;word-break:break-all">${(p.images || []).map(escapeHtml).join('<br>')}</div></td>
      <td>
        <button class="btn btn-secondary btn-sm" onclick="viewPodLog('${escapeHtml(p.name)}', '${escapeHtml(p.containers[0] || '')}')">查看日志</button>
      </td>
    </tr>`;
  }).join('');

  $('podsTableBody').innerHTML = rows;
}

function filterPodsTable() {
  const keyword = $('podSearchInput').value.toLowerCase().trim();
  if (!keyword) {
    renderPodsTable(currentPods);
    return;
  }
  const filtered = currentPods.filter((p) => p.name.toLowerCase().includes(keyword) || (p.ip && p.ip.includes(keyword)));
  renderPodsTable(filtered);
}

window.viewPodLog = async function (podName, containerName) {
  const srvId = $('podServerSelect').value;
  const ns = $('podNamespaceSelect').value;
  $('podLogTitle').textContent = `Pod 日志: ${podName}`;
  $('podLogContent').innerHTML = '<span class="log-debug">正在获取最新日志...</span>';
  $('podLogModal').classList.remove('hidden');

  $('refreshLogBtn').onclick = () => window.viewPodLog(podName, containerName);

  const res = await api('GET', `/api/k8s/pod-logs?serverId=${encodeURIComponent(srvId)}&namespace=${encodeURIComponent(ns)}&pod=${encodeURIComponent(podName)}&container=${encodeURIComponent(containerName)}&tail=200`);
  if (!res.ok) {
    $('podLogContent').innerHTML = `<span class="log-err">获取日志失败: ${escapeHtml(res.data.error || '未知错误')}</span>`;
    return;
  }
  const raw = res.data.logs || '';
  window._rawPodLog = raw;
  $('podLogContent').innerHTML = colorizeLog(raw);
};

// ---------------- 服务器管理 (长连接池保持) ----------------
function renderServerCards() {
  if (state.servers.length === 0) {
    $('serverCardsList').innerHTML = '<div class="config-hint">尚未配置服务器，请点击右上角添加。</div>';
    return;
  }
  $('serverCardsList').innerHTML = state.servers.map((s) => `
    <div class="item-card">
      <div class="item-card-header">
        <span class="item-card-title">${escapeHtml(s.name)}</span>
        <div>
          <button class="btn btn-secondary btn-sm" onclick="testOneServer('${s.id}')">⚡ 测试长连接</button>
          <button class="btn btn-secondary btn-sm" onclick="editServer('${s.id}')">编辑</button>
          <button class="btn btn-danger btn-sm" onclick="deleteServer('${s.id}')">删除</button>
        </div>
      </div>
      <div class="item-card-desc">连接信息: <code>${escapeHtml(s.user)}@${escapeHtml(s.host)}:${s.port || 22}</code> (认证: ${s.authType === 'key' ? '私钥' : '密码'})</div>
      <div class="item-card-desc">连接池机制: 常驻长连接保持，免重复握手验证</div>
    </div>
  `).join('');
}

window.testOneServer = async function (srvId) {
  const srv = state.servers.find((s) => s.id === srvId);
  if (!srv) return;
  const res = await api('POST', '/api/servers/test', srv);
  if (res.ok) {
    alert(`✔ ${res.data.message || '连接测试成功！'}`);
  } else {
    alert(`✖ 连接失败: ${res.data.error || '未知错误'}`);
  }
};

function openServerModal(srv) {
  $('editServerId').value = srv ? srv.id : '';
  $('editServerName').value = srv ? srv.name : '';
  $('editServerHost').value = srv ? srv.host : '';
  $('editServerPort').value = srv ? srv.port : '22';
  $('editServerUser').value = srv ? srv.user : 'root';
  $('editServerAuthType').value = srv ? (srv.authType || 'password') : 'password';
  $('editServerPassword').value = srv ? srv.password : '';
  $('editServerKeyPath').value = srv ? srv.keyPath : '';

  $('pwdGroup').classList.toggle('hidden', srv && srv.authType === 'key');
  $('keyGroup').classList.toggle('hidden', !srv || srv.authType !== 'key');
  $('serverModal').classList.remove('hidden');
}

window.editServer = function (id) {
  const s = state.servers.find((x) => x.id === id);
  if (s) openServerModal(s);
};

window.deleteServer = async function (id) {
  if (!confirm('确定要删除此服务器配置吗？')) return;
  const res = await api('DELETE', `/api/servers/${encodeURIComponent(id)}`);
  if (res.ok) await refreshConfigs();
};

async function testServerConnection() {
  const srv = {
    id: $('editServerId').value,
    name: $('editServerName').value,
    host: $('editServerHost').value.trim(),
    port: parseInt($('editServerPort').value) || 22,
    user: $('editServerUser').value.trim(),
    authType: $('editServerAuthType').value,
    password: $('editServerPassword').value,
    keyPath: $('editServerKeyPath').value.trim(),
  };
  if (!srv.host || !srv.user) return alert('请先填写主机和用户名');

  $('testServerConnBtn').textContent = '测试中...';
  $('testServerConnBtn').disabled = true;
  const res = await api('POST', '/api/servers/test', srv);
  $('testServerConnBtn').textContent = '测试连接';
  $('testServerConnBtn').disabled = false;

  if (res.ok) {
    alert(`✔ ${res.data.message || '连接成功！'}`);
  } else {
    alert(`✖ 连接失败: ${res.data.error}`);
  }
}

async function saveServer() {
  const srv = {
    id: $('editServerId').value || 'srv-' + Date.now(),
    name: $('editServerName').value.trim() || $('editServerHost').value.trim(),
    host: $('editServerHost').value.trim(),
    port: parseInt($('editServerPort').value) || 22,
    user: $('editServerUser').value.trim(),
    authType: $('editServerAuthType').value,
    password: $('editServerPassword').value,
    keyPath: $('editServerKeyPath').value.trim(),
  };
  if (!srv.host || !srv.user) return alert('主机 IP 与用户名必填');

  const res = await api('POST', '/api/servers', srv);
  if (!res.ok) return alert('保存失败: ' + res.data.error);

  $('serverModal').classList.add('hidden');
  await refreshConfigs();
}

// ---------------- 应用配置管理 (独立目录配置) ----------------
function renderAppCards() {
  if (state.apps.length === 0) {
    $('appCardsList').innerHTML = '<div class="config-hint">尚未配置应用，请点击右上角添加。</div>';
    return;
  }
  $('appCardsList').innerHTML = state.apps.map((a) => `
    <div class="item-card">
      <div class="item-card-header">
        <span class="item-card-title">${escapeHtml(a.name)} (${escapeHtml(a.type)})</span>
        <div>
          <button class="btn btn-secondary btn-sm" onclick="editApp('${a.id}')">编辑</button>
          <button class="btn btn-danger btn-sm" onclick="deleteApp('${a.id}')">删除</button>
        </div>
      </div>
      <div class="item-card-desc"><strong>产物目录:</strong> <code>${escapeHtml(a.artifactDir || '-')}</code></div>
      <div class="item-card-desc"><strong>构建目录:</strong> <code>${escapeHtml(a.buildDir || '-')}</code></div>
      <div class="item-card-desc"><strong>K8s 配置:</strong> <code>${escapeHtml(a.k8sYamlFile ? (a.k8sYamlDir ? a.k8sYamlDir + '/' + a.k8sYamlFile : a.k8sYamlFile) : (a.k8sYamlDir || '-'))}</code></div>
      <div class="item-card-desc"><strong>目标镜像:</strong> <code>${escapeHtml(a.image || '-')}</code></div>
      <div class="item-card-desc"><strong>K8s 部署:</strong> ns: <code>${escapeHtml(a.namespace)}</code>, deploy: <code>${escapeHtml(a.deployment)}</code></div>
    </div>
  `).join('');
}

function openAppModal(app) {
  $('editAppId').value = app ? app.id : '';
  $('editAppName').value = app ? app.name : '';
  $('editAppType').value = app ? (app.type || 'jar') : 'jar';
  $('editAppImage').value = app ? (app.image || '') : '';
  $('editAppArtifactDir').value = app ? (app.artifactDir || '') : '/data/packages';
  $('editAppBuildDir').value = app ? (app.buildDir || '') : '/data/docker-build';
  $('editAppK8sYamlDir').value = app ? (app.k8sYamlDir || '') : '/data/k8s-yaml';
  $('editAppK8sYamlFile').value = app ? (app.k8sYamlFile || '') : '';
  $('editAppBuildCmd').value = app ? (app.buildCmd || '') : 'cd {{.BuildDir}} && docker build -t {{.Image}}:{{.Tag}} .';
  $('editAppPushCmd').value = app ? (app.pushCmd || '') : '';
  $('editAppDeployCmd').value = app ? (app.deployCmd || '') : 'kubectl apply -f {{.K8sYamlPath}}';
  $('editAppNamespace').value = app ? (app.namespace || '') : 'default';
  $('editAppDeployment').value = app ? (app.deployment || '') : '';
  $('editAppContainer').value = app ? (app.container || '') : '';

  // 渲染绑定服务器选项
  $('editAppServerId').innerHTML = state.servers.map((s) => `
    <option value="${s.id}" ${app && app.serverId === s.id ? 'selected' : ''}>${escapeHtml(s.name)} (${escapeHtml(s.host)})</option>
  `).join('');

  $('appModal').classList.remove('hidden');
}

window.editApp = function (id) {
  const app = state.apps.find((x) => x.id === id);
  if (app) openAppModal(app);
};

window.deleteApp = async function (id) {
  if (!confirm('确定要删除此应用配置吗？')) return;
  const res = await api('DELETE', `/api/apps/${encodeURIComponent(id)}`);
  if (res.ok) await refreshConfigs();
};

async function saveApp() {
  const name = $('editAppName').value.trim();
  if (!name) return alert('应用名称必填');

  const app = {
    id: $('editAppId').value || name,
    name,
    type: $('editAppType').value,
    serverId: $('editAppServerId').value,
    image: $('editAppImage').value.trim(),
    artifactDir: $('editAppArtifactDir').value.trim(),
    buildDir: $('editAppBuildDir').value.trim(),
    k8sYamlDir: $('editAppK8sYamlDir').value.trim(),
    k8sYamlFile: $('editAppK8sYamlFile').value.trim(),
    buildCmd: $('editAppBuildCmd').value.trim(),
    pushCmd: $('editAppPushCmd').value.trim(),
    deployCmd: $('editAppDeployCmd').value.trim(),
    namespace: $('editAppNamespace').value.trim() || 'default',
    deployment: $('editAppDeployment').value.trim() || name,
    container: $('editAppContainer').value.trim() || name,
    rolloutTimeout: 300,
    autoRollback: true,
  };

  const res = await api('POST', '/api/apps', app);
  if (!res.ok) return alert('保存应用失败: ' + res.data.error);

  $('appModal').classList.add('hidden');
  await refreshConfigs();
}

// ---------------- 发布历史与指标统计 (需求 1 优化) ----------------
let rawHistoryList = [];

async function loadHistory() {
  const appId = $('historyAppFilter').value;
  $('historyTableBody').innerHTML = '<tr><td colspan="8" class="td-empty">正在加载历史发布记录...</td></tr>';

  const res = await api('GET', `/api/releases?limit=100${appId ? '&app=' + encodeURIComponent(appId) : ''}`);
  if (!res.ok) {
    $('historyTableBody').innerHTML = `<tr><td colspan="8" class="td-empty" style="color:var(--danger)">加载失败: ${escapeHtml(res.data.error || '网络异常')}</td></tr>`;
    return;
  }

  rawHistoryList = res.data || [];
  renderHistoryMetrics(rawHistoryList);
  filterHistoryList();
}

function renderHistoryMetrics(list) {
  const total = list.length;
  const successCount = list.filter((r) => r.status === 'success').length;
  const failedCount = list.filter((r) => r.status === 'failed').length;
  const forceCount = list.filter((r) => r.force).length;
  const rate = total > 0 ? Math.round((successCount / total) * 100) : 100;

  $('metricTotal').textContent = total;
  $('metricSuccessRate').textContent = rate + '%';
  $('metricForceCount').textContent = forceCount;
  $('metricFailedCount').textContent = failedCount;
}

function filterHistoryList() {
  const statusFilter = $('historyStatusFilter').value;
  const keyword = $('historySearchInput').value.toLowerCase().trim();

  let filtered = rawHistoryList;

  if (statusFilter === 'success') {
    filtered = filtered.filter((r) => r.status === 'success');
  } else if (statusFilter === 'failed') {
    filtered = filtered.filter((r) => r.status === 'failed');
  } else if (statusFilter === 'force') {
    filtered = filtered.filter((r) => r.force);
  }

  if (keyword) {
    filtered = filtered.filter((r) =>
      (r.tag && r.tag.toLowerCase().includes(keyword)) ||
      (r.app && r.app.toLowerCase().includes(keyword)) ||
      (r.fileName && r.fileName.toLowerCase().includes(keyword)) ||
      (r.remark && r.remark.toLowerCase().includes(keyword))
    );
  }

  renderHistoryTable(filtered);
}

function renderHistoryTable(list) {
  if (list.length === 0) {
    $('historyTableBody').innerHTML = '<tr><td colspan="8" class="td-empty">未匹配到发布记录</td></tr>';
    return;
  }

  $('historyTableBody').innerHTML = list.map((r) => {
    const isSuccess = r.status === 'success';
    const statusIcon = isSuccess ? '✔' : r.status === 'running' ? '⏳' : '✖';
    const statusText = isSuccess ? '发布成功' : r.status === 'running' ? '进行中' : '发布失败';
    const statusCls = isSuccess ? 'status-success' : r.status === 'running' ? 'status-running' : 'status-failed';

    const appObj = state.apps.find((a) => a.id === r.app);
    const appType = appObj ? appObj.type : 'app';

    // 计算耗时
    let durationStr = '-';
    if (r.createdAt && r.finishedAt) {
      const ms = new Date(r.finishedAt) - new Date(r.createdAt);
      const s = Math.max(1, Math.round(ms / 1000));
      durationStr = s >= 60 ? `${Math.floor(s / 60)}m${s % 60}s` : `${s}s`;
    }

    const shaShort = r.fileSha256 ? r.fileSha256.substring(0, 10) : '';

    return `
      <tr>
        <td>
          <div style="font-weight:600;color:#1e293b">${fmtTime(r.createdAt)}</div>
        </td>
        <td>
          <div class="app-badge-inline">
            <strong>${escapeHtml(r.app)}</strong>
            <span class="app-type-tag">${escapeHtml(appType)}</span>
          </div>
        </td>
        <td>
          <div class="history-tag-chip">
            <span>${escapeHtml(r.tag)}</span>
            <button class="copy-tag-btn" title="复制版本号" onclick="copyText('${escapeHtml(r.tag)}')">📋</button>
          </div>
          ${r.force ? '<div class="force-tag-warn">⚠️ 强制覆盖版本</div>' : ''}
        </td>
        <td>
          <span class="status-pill ${statusCls}">
            <span>${statusIcon}</span> ${statusText}
          </span>
          ${r.error ? `<div style="font-size:11px;color:var(--danger);margin-top:4px;max-width:180px;word-break:break-all">${escapeHtml(r.error)}</div>` : ''}
        </td>
        <td>
          <div class="artifact-summary">
            <span class="artifact-name">${escapeHtml(r.fileName || '-')}</span>
            <span class="artifact-meta">
              ${fmtSize(r.fileSize)} ${shaShort ? '· SHA: ' + shaShort + '...' : ''}
            </span>
          </div>
        </td>
        <td>
          <span class="duration-chip">${durationStr}</span>
        </td>
        <td style="color:#475569;font-size:12px;max-width:180px;word-break:break-all">
          ${escapeHtml(r.remark || '-')}
        </td>
        <td>
          <div class="history-row-actions">
            <button class="btn btn-secondary btn-sm" onclick="openReleaseLogModal('${r.id}', '${escapeHtml(r.app)}', '${escapeHtml(r.tag)}')">
              📋 查看日志
            </button>
            <button class="btn btn-secondary btn-sm" title="填入发布面板" onclick="reuseReleaseTag('${escapeHtml(r.app)}', '${escapeHtml(r.tag)}')">
              🚀 选用
            </button>
          </div>
        </td>
      </tr>
    `;
  }).join('');
}

window.copyText = function (text) {
  navigator.clipboard.writeText(text).then(() => {
    alert(`已复制版本号: ${text}`);
  }).catch(() => {
    prompt('请手动复制:', text);
  });
};

window.reuseReleaseTag = function (appId, tag) {
  // 切换到第一页并填充应用和 tag
  document.querySelector('.tab-btn[data-tab="deploy"]').click();
  $('deployAppSelect').value = appId;
  $('tagInput').value = tag;
  updatePathPreview();
};

window.openReleaseLogModal = async function (id, app, tag) {
  $('releaseLogModalTitle').textContent = `发布日志: ${app} [${tag}]`;
  $('releaseLogModalSub').textContent = `任务编号: ${id}`;
  $('releaseLogModalBody').innerHTML = '<span class="log-debug">正在获取构建日志...</span>';
  $('downloadReleaseLogLink').href = `/api/releases/${id}/log`;
  $('releaseLogModal').classList.remove('hidden');

  let rawLog = '';
  $('copyReleaseLogBtn').onclick = () => {
    navigator.clipboard.writeText(rawLog).then(() => {
      alert('日志已复制到剪贴板！');
    });
  };

  const res = await fetch(`/api/releases/${id}/log`);
  if (!res.ok) {
    $('releaseLogModalBody').innerHTML = `<span class="log-err">获取日志失败 HTTP ${res.status}</span>`;
    return;
  }
  rawLog = await res.text();
  $('releaseLogModalBody').innerHTML = colorizeLog(rawLog);
};

$('closeReleaseLogBtn').addEventListener('click', () => {
  $('releaseLogModal').classList.add('hidden');
});

$('historyAppFilter').addEventListener('change', loadHistory);
$('historyStatusFilter').addEventListener('change', filterHistoryList);
$('historySearchInput').addEventListener('input', filterHistoryList);
$('refreshHistoryBtn').addEventListener('click', loadHistory);
