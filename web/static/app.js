'use strict';

const $ = (id) => document.getElementById(id);
const CHUNK_SIZE = 4 * 1024 * 1024;
const TAG_RE = /^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$/;
const PREFS_KEY = 'btd_user_prefs_v1';

const state = {
  apps: [],
  servers: [],
  activeServerIds: [],
  activeServers: [],
  deployMode: 'upload', // 'upload' | 'local_build'
  localArtifactInfo: null,
  selectedFile: null,
  uploadId: null,
  releaseId: null,
  eventSource: null,
  isDeploying: false,
  pendingReleaseParams: null,
  prefs: {
    selectedAppId: '',
    deployModeByApp: {},
    useDefaultArtifactByApp: {},
    lastTagByApp: {},
  },
};

function loadPrefs() {
  try {
    const raw = localStorage.getItem(PREFS_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      state.prefs = {
        selectedAppId: parsed.selectedAppId || '',
        deployModeByApp: parsed.deployModeByApp || {},
        useDefaultArtifactByApp: parsed.useDefaultArtifactByApp || {},
        lastTagByApp: parsed.lastTagByApp || {},
      };
    }
  } catch (_) {}
}

function savePrefs() {
  try {
    localStorage.setItem(PREFS_KEY, JSON.stringify(state.prefs));
  } catch (_) {}
}

async function pickLocalPath(mode, promptText) {
  const res = await api('POST', '/api/local/pick-path', { mode, prompt: promptText });
  if (!res.ok) {
    alert(`打开本地文件选择器失败: ${res.data.error || '未知错误'}`);
    return null;
  }
  if (res.data.canceled || !res.data.path) {
    return null;
  }
  return res.data.path;
}

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
function clearBrowserAutofill() {
  const badValues = ['admin', 'root', 'deploy'];
  ['tagInput', 'remarkInput'].forEach((id) => {
    const el = $(id);
    if (el && badValues.includes(el.value.trim().toLowerCase())) {
      el.value = '';
    }
  });
}

document.addEventListener('DOMContentLoaded', async () => {
  loadPrefs();
  setupTabs();
  setupDropZone();
  setupButtons();
  $('remarkInput').value = '';
  $('tagInput').value = '';
  // 多重延迟防御浏览器密码管理器把保存的 admin 自动填入 Tag 或备注输入框
  [50, 150, 400, 900].forEach((ms) => setTimeout(clearBrowserAutofill, ms));
  await refreshConfigs();
  await updateConnStatus();
  setInterval(updateConnStatus, 10000);
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
  const prevVal = $('deployAppSelect').value || state.prefs.selectedAppId;
  const opts = state.apps.map((a) => `<option value="${a.id}">${escapeHtml(a.name)} (${escapeHtml(a.type)})</option>`).join('');
  $('deployAppSelect').innerHTML = opts || '<option value="">暂无应用，请先在配置中添加</option>';
  if (prevVal && state.apps.some((a) => a.id === prevVal)) {
    $('deployAppSelect').value = prevVal;
  }
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
  // 保存上次选择的应用
  state.prefs.selectedAppId = appId;
  savePrefs();

  // 恢复该应用上次选择的构建/上传模式
  const savedMode = state.prefs.deployModeByApp[appId];
  if (savedMode && (savedMode === 'upload' || savedMode === 'local_build') && savedMode !== state.deployMode) {
    switchDeployMode(savedMode, false);
  }

  $('appPathPreview').classList.remove('hidden');
  const srv = state.servers.find((s) => s.id === app.serverId);
  $('pvServer').textContent = srv ? `${srv.name} (${srv.user}@${srv.host})` : (app.serverId || '-');
  $('pvArtifactDir').textContent = app.artifactDir || '未配置';
  $('pvBuildDir').textContent = app.buildDir || '未配置';
  if (app.k8sYamlDir && app.k8sYamlFile) {
    $('pvYamlDir').innerHTML = `<span>${escapeHtml(app.k8sYamlDir)}/</span><strong style="color:#2563eb">${escapeHtml(app.k8sYamlFile)}</strong>`;
  } else {
    $('pvYamlDir').textContent = app.k8sYamlFile || app.k8sYamlDir || '未配置';
  }
  const curTag = $('tagInput').value.trim() || '<Tag>';
  $('pvImage').innerHTML = app.image
    ? `<span>${escapeHtml(app.image)}:</span><strong style="color:#059669">${escapeHtml(curTag)}</strong>`
    : '未配置';

  // 更新本地编译配置预览
  $('pvLocalDir').textContent = app.localProjectDir || '未配置 (可在应用配置中设置)';
  $('pvLocalCmd').textContent = app.localBuildCmd || '未配置 (请点击右上角“修改编译配置”填写)';
  $('pvLocalArtifact').textContent = app.localArtifactPath || '未配置';

  inspectLocalArtifact(app.id);
}

async function inspectLocalArtifact(appId) {
  if (!appId) return;
  const res = await api('GET', `/api/apps/${encodeURIComponent(appId)}/local-artifact`);
  if (!res.ok) return;
  const info = res.data;
  state.localArtifactInfo = info;

  const quickBox = $('localArtifactQuickBox');
  const checkEl = $('useDefaultArtifactCheck');
  const badgeEl = $('localArtifactStatusBadge');
  const pathEl = $('localArtifactPathText');

  quickBox.classList.remove('hidden');

  if (!info.configured) {
    quickBox.classList.add('missing');
    badgeEl.textContent = '未设置默认产物路径';
    pathEl.textContent = '可点击右侧“📄 选择文件”选取本地产物并自动记住该路径';
    checkEl.checked = false;
    checkEl.disabled = true;
    updateDropZoneVisual();
    return;
  }

  pathEl.textContent = info.path;

  if (info.exists) {
    quickBox.classList.remove('missing');
    badgeEl.textContent = `已就绪 · ${fmtSize(info.size)} (${fmtTime(info.modTime)})`;
    checkEl.disabled = false;
    const savedPref = state.prefs.useDefaultArtifactByApp[appId];
    if (!state.selectedFile) {
      checkEl.checked = savedPref !== undefined ? !!savedPref : true;
    }
  } else {
    quickBox.classList.add('missing');
    badgeEl.textContent = '本地暂未生成此文件 (可点击右侧“📄 选择文件”重新指定)';
    checkEl.checked = false;
    checkEl.disabled = true;
  }
  updateDropZoneVisual();
}

function switchDeployMode(mode, persist = true) {
  state.deployMode = mode;
  const appId = $('deployAppSelect') ? $('deployAppSelect').value : '';
  if (persist && appId) {
    state.prefs.deployModeByApp[appId] = mode;
    savePrefs();
  }
  document.querySelectorAll('#deployModeTabs .mode-tab-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.mode === mode);
  });

  const isLocalBuild = mode === 'local_build';
  $('directUploadPanel').classList.toggle('hidden', isLocalBuild);
  $('localBuildPanel').classList.toggle('hidden', !isLocalBuild);

  const startBtn = $('startDeployBtn');
  if (isLocalBuild) {
    startBtn.classList.add('mode-local-build');
    startBtn.innerHTML = '<span class="btn-icon">⚡</span> 执行本地编译并上传部署';
  } else {
    startBtn.classList.remove('mode-local-build');
    startBtn.innerHTML = '<span class="btn-icon">🚀</span> 开始上传并构建部署';
  }
}

function updateDropZoneVisual() {
  const dropZone = $('dropZone');
  const useDefault = $('useDefaultArtifactCheck') && $('useDefaultArtifactCheck').checked;
  if (state.selectedFile) {
    dropZone.classList.add('has-file');
    dropZone.classList.remove('disabled-zone');
    $('dropText').textContent = `已选择手动文件: ${state.selectedFile.name} (${fmtSize(state.selectedFile.size)})`;
  } else if (useDefault && state.localArtifactInfo && state.localArtifactInfo.exists) {
    dropZone.classList.remove('has-file');
    dropZone.classList.add('disabled-zone');
    $('dropText').textContent = '当前已勾选使用上方本地默认产物 (也可点击此处重选其他文件)';
  } else {
    dropZone.classList.remove('has-file');
    dropZone.classList.remove('disabled-zone');
    $('dropText').textContent = '点击选择 或 将编译好的产物拖拽至此处';
  }
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
  if ($('useDefaultArtifactCheck')) {
    $('useDefaultArtifactCheck').checked = false;
  }
  updateDropZoneVisual();

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

  // 构建与上传模式切换绑定
  document.querySelectorAll('#deployModeTabs .mode-tab-btn').forEach((btn) => {
    btn.addEventListener('click', () => switchDeployMode(btn.dataset.mode));
  });

  if ($('useDefaultArtifactCheck')) {
    $('useDefaultArtifactCheck').addEventListener('change', (e) => {
      if (e.target.checked) {
        state.selectedFile = null;
        $('fileInput').value = '';
      }
      const appId = $('deployAppSelect').value;
      if (appId) {
        state.prefs.useDefaultArtifactByApp[appId] = e.target.checked;
        savePrefs();
      }
      updateDropZoneVisual();
    });
  }

  // 发布页：直接点击“📄 选择文件”选取本地产物文件并自动保存到当前应用配置
  if ($('quickPickLocalArtifactBtn')) {
    $('quickPickLocalArtifactBtn').addEventListener('click', async () => {
      const appId = $('deployAppSelect').value;
      const app = state.apps.find((a) => a.id === appId);
      if (!app) return alert('请先选择目标应用');
      const btn = $('quickPickLocalArtifactBtn');
      const origText = btn.textContent;
      btn.textContent = '选择中...';
      btn.disabled = true;
      const picked = await pickLocalPath('file', `请选择 ${app.name} 的本地编译产物文件`);
      btn.textContent = origText;
      btn.disabled = false;
      if (!picked) return;

      app.localArtifactPath = picked;
      const saveRes = await api('POST', '/api/apps', app);
      if (saveRes.ok) {
        state.selectedFile = null;
        $('fileInput').value = '';
        state.prefs.useDefaultArtifactByApp[appId] = true;
        savePrefs();
        await inspectLocalArtifact(appId);
        // 自动尝试从文件名提取版本号
        if (!$('tagInput').value.trim()) {
          const baseName = picked.split(/[\\/]/).pop() || '';
          const match = baseName.match(/(\d+\.\d+(?:\.\d+)?)/);
          if (match) {
            $('tagInput').value = 'v' + match[1];
          }
        }
      }
    });
  }

  // 编辑应用弹窗：点击“📂 选择目录”或“📄 选择文件”
  if ($('pickProjectDirBtn')) {
    $('pickProjectDirBtn').addEventListener('click', async () => {
      const btn = $('pickProjectDirBtn');
      const orig = btn.textContent;
      btn.textContent = '打开中...';
      btn.disabled = true;
      const picked = await pickLocalPath('dir', '请选择本地项目源码根目录');
      btn.textContent = orig;
      btn.disabled = false;
      if (picked) {
        $('editAppLocalProjectDir').value = picked;
      }
    });
  }

  if ($('pickArtifactFileBtn')) {
    $('pickArtifactFileBtn').addEventListener('click', async () => {
      const btn = $('pickArtifactFileBtn');
      const orig = btn.textContent;
      btn.textContent = '打开中...';
      btn.disabled = true;
      const picked = await pickLocalPath('file', '请选择本地编译产出文件 (.jar / .tar.gz / .zip)');
      btn.textContent = orig;
      btn.disabled = false;
      if (picked) {
        $('editAppLocalArtifactPath').value = picked;
      }
    });
  }

  if ($('appConfigSearchInput')) {
    $('appConfigSearchInput').addEventListener('input', () => renderAppCards());
  }

  if ($('quickEditAppBuildBtn')) {
    $('quickEditAppBuildBtn').addEventListener('click', () => {
      const appId = $('deployAppSelect').value;
      if (appId) window.editApp(appId);
    });
  }

  if ($('cancelReleaseBtn')) {
    $('cancelReleaseBtn').addEventListener('click', async () => {
      if (!state.releaseId) return;
      await api('POST', `/api/releases/${state.releaseId}/cancel`);
    });
  }

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
  if ($('connectSrvBtn')) {
    $('connectSrvBtn').addEventListener('click', connectCurrentServer);
  }
  $('disconnectAllBtn').addEventListener('click', disconnectAllConnections);
  $('tagInput').addEventListener('input', () => {
    const appId = $('deployAppSelect').value;
    const app = state.apps.find((a) => a.id === appId);
    if (app && app.image) {
      const curTag = $('tagInput').value.trim() || '<Tag>';
      $('pvImage').innerHTML = `<span>${escapeHtml(app.image)}:</span><strong style="color:#059669">${escapeHtml(curTag)}</strong>`;
    }
  });

  // 应用配置弹窗联动：输入应用名或点击“按应用名自动填充”时自动生成独立构建目录、独立YAML文件名与独立镜像名
  if ($('autoFillAppPathsBtn')) {
    $('autoFillAppPathsBtn').addEventListener('click', () => autoFillAppPathsByName(true));
  }
  if ($('editAppName')) {
    $('editAppName').addEventListener('input', () => {
      if (!$('editAppId').value) {
        autoFillAppPathsByName(false);
      }
    });
  }
  if ($('tplYamlApplyBtn')) {
    $('tplYamlApplyBtn').addEventListener('click', () => {
      $('editAppDeployCmd').value = "kubectl apply -f {{.K8sYamlPath}}";
    });
  }
  if ($('tplSetImageBtn')) {
    $('tplSetImageBtn').addEventListener('click', () => {
      $('editAppDeployCmd').value = "kubectl -n {{.Namespace}} set image deployment/{{.Deployment}} {{.Container}}={{.Image}}:{{.Tag}}";
    });
  }
}

async function onStartDeployClick() {
  if (state.isDeploying) return;
  const appId = $('deployAppSelect').value;
  const tag = $('tagInput').value.trim();
  const remark = $('remarkInput').value.trim();

  if (!appId) return alert('请先选择要发布的应用');
  const app = state.apps.find((a) => a.id === appId);

  const isLocalBuild = state.deployMode === 'local_build';
  const useDefaultArtifact = !isLocalBuild && !state.selectedFile && $('useDefaultArtifactCheck') && $('useDefaultArtifactCheck').checked;

  // 保存当前应用的发布模式与选项，以便下次打开直接恢复
  state.prefs.selectedAppId = appId;
  state.prefs.deployModeByApp[appId] = state.deployMode;
  state.prefs.useDefaultArtifactByApp[appId] = !!useDefaultArtifact;
  if (tag) {
    state.prefs.lastTagByApp[appId] = tag;
  }
  savePrefs();

  if (isLocalBuild) {
    if (!app || !app.localBuildCmd || !app.localArtifactPath) {
      return alert('当前应用尚未配置“本地编译命令”或“本地编译产出文件路径”，请先点击“修改编译配置”填写！');
    }
  } else {
    if (!state.selectedFile && !useDefaultArtifact) {
      return alert('请先选择本地构建产物文件，或切换至“⚡ 本地一键编译并上传”模式');
    }
  }

  if (!tag || !TAG_RE.test(tag)) return alert('请输入合法的镜像 Tag (只能包含字母、数字、点、减号、下划线)');

  appendLog(`\n[本地] 准备发布应用: ${appId}, Tag: ${tag} (${isLocalBuild ? '本地一键编译并上传' : '直接上传已有产物'})`);
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
    state.pendingReleaseParams = { appId, tag, remark, isLocalBuild, useDefaultArtifact };
    showConflictModal(conflicts, tag);
    return;
  }

  // 无冲突，直接开始
  startDeployPipeline(appId, tag, remark, false, isLocalBuild, useDefaultArtifact);
}

function showConflictModal(conflicts, tag) {
  const itemsHtml = conflicts.map((c) => `<div class="conflict-item">⚠️ [${c.source}] ${escapeHtml(c.message)}</div>`).join('');
  $('conflictItems').innerHTML = itemsHtml;
  $('conflictModal').classList.remove('hidden');
}

async function onConfirmConflictDeploy() {
  $('conflictModal').classList.add('hidden');
  if (!state.pendingReleaseParams) return;
  const { appId, tag, remark, isLocalBuild, useDefaultArtifact } = state.pendingReleaseParams;
  appendLog(`[确认覆盖] 用户确认强制覆盖同名镜像版本: ${tag}`);
  startDeployPipeline(appId, tag, remark, true, isLocalBuild, useDefaultArtifact);
}

async function startDeployPipeline(appId, tag, remark, force, isLocalBuild, useDefaultArtifact) {
  state.isDeploying = true;
  $('startDeployBtn').disabled = true;
  $('cancelReleaseBtn').classList.remove('hidden');
  updateStatus('running', isLocalBuild ? '正在执行本地编译...' : '正在准备产物...');

  try {
    let upId = '';
    if (!isLocalBuild && state.selectedFile) {
      // 2. 手动选择的本地文件分片上传至本地暂存区
      updateStatus('running', '正在上传产物...');
      upId = await uploadFileChunks(state.selectedFile);
      state.uploadId = upId;
    }

    // 3. 发起发布任务
    appendLog(isLocalBuild
      ? '[本地] 触发本地一键编译与远程构建部署流水线...'
      : '[本地] 产物就绪，触发远程服务器构建部署流水线...');
    const startRes = await api('POST', '/api/releases', {
      appId,
      tag,
      uploadId: upId,
      runLocalBuild: !!isLocalBuild,
      useLocalArtifact: !!useDefaultArtifact,
      force,
      remark,
    });

    if (!startRes.ok) {
      throw new Error(startRes.data.error || '启动发布流水线失败');
    }

    state.releaseId = startRes.data.id;
    appendLog(`[成功] 流水线已启动，任务 ID: ${state.releaseId}`);
    $('remarkInput').value = '';
    updateConnStatus();

    // 4. 监听 SSE 实时进度与日志
    listenReleaseEvents(state.releaseId);
  } catch (err) {
    appendLog(`[错误] ${err.message}`);
    updateStatus('failed', '发布失败');
    state.isDeploying = false;
    $('startDeployBtn').disabled = false;
    $('cancelReleaseBtn').classList.add('hidden');
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
    $('cancelReleaseBtn').classList.add('hidden');
    $('uploadProgressContainer').classList.add('hidden');
    updatePathPreview();
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
        <div style="display:flex;gap:6px;">
          <button class="btn btn-secondary btn-sm" onclick="viewPodLog('${escapeHtml(p.name)}')">查看日志</button>
          <button class="btn btn-secondary btn-sm" title="滚动重启该 Deployment" onclick="restartPodDeployment('${escapeHtml(p.name)}')">🔄 重启</button>
        </div>
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

let podLogTimer = null;
let currentViewingPod = null;
let currentViewingContainer = '';

function stopPodLogFollow() {
  if (podLogTimer) {
    clearInterval(podLogTimer);
    podLogTimer = null;
  }
}

window.viewPodLog = async function (podName, containerName) {
  const srvId = $('podServerSelect').value;
  const ns = $('podNamespaceSelect').value;
  currentViewingPod = podName;

  const podObj = currentPods.find((p) => p.name === podName);
  const containers = (podObj && podObj.containers) ? podObj.containers : [];

  const wrapEl = $('podLogContainerWrap');
  const selectEl = $('podLogContainerSelect');

  if (containers.length > 1) {
    wrapEl.classList.remove('hidden');
    selectEl.innerHTML = containers.map((c) => `<option value="${escapeHtml(c)}"${c === containerName ? ' selected' : ''}>${escapeHtml(c)}</option>`).join('');
    currentViewingContainer = containerName || selectEl.value;
    selectEl.onchange = () => {
      currentViewingContainer = selectEl.value;
      fetchPodLogContent(srvId, ns, currentViewingPod, currentViewingContainer);
    };
  } else {
    wrapEl.classList.add('hidden');
    currentViewingContainer = containerName || (containers[0] || '');
  }

  $('podLogTitle').textContent = `Pod 日志: ${podName}`;
  $('podLogContent').innerHTML = '<span class="log-debug">正在获取最新日志...</span>';
  $('podLogModal').classList.remove('hidden');

  $('refreshLogBtn').onclick = () => fetchPodLogContent(srvId, ns, currentViewingPod, currentViewingContainer);

  $('autoFollowPodLog').onchange = (e) => {
    stopPodLogFollow();
    if (e.target.checked) {
      podLogTimer = setInterval(() => {
        fetchPodLogContent(srvId, ns, currentViewingPod, currentViewingContainer, true);
      }, 2500);
    }
  };

  await fetchPodLogContent(srvId, ns, currentViewingPod, currentViewingContainer);
};

async function fetchPodLogContent(srvId, ns, podName, containerName, isAuto) {
  const res = await api('GET', `/api/k8s/pod-logs?serverId=${encodeURIComponent(srvId)}&namespace=${encodeURIComponent(ns)}&pod=${encodeURIComponent(podName)}&container=${encodeURIComponent(containerName)}&tail=200`);
  if (!res.ok) {
    if (!isAuto) {
      $('podLogContent').innerHTML = `<span class="log-err">获取日志失败: ${escapeHtml(res.data.error || '未知错误')}</span>`;
    }
    return;
  }
  const raw = res.data.logs || '';
  window._rawPodLog = raw;
  $('podLogContent').innerHTML = colorizeLog(raw);
  if ($('autoFollowPodLog').checked) {
    const el = $('podLogContent');
    el.scrollTop = el.scrollHeight;
  }
}

$('closePodLogBtn').addEventListener('click', () => {
  stopPodLogFollow();
  $('autoFollowPodLog').checked = false;
  $('podLogModal').classList.add('hidden');
});

window.restartPodDeployment = async function (podName) {
  const srvId = $('podServerSelect').value;
  const ns = $('podNamespaceSelect').value;

  // 推断 Deployment 名称：匹配 app 中的 deployment 或者按 pod 名称切分
  let deployName = '';
  const matchedApp = state.apps.find((a) => a.deployment && podName.startsWith(a.deployment));
  if (matchedApp) {
    deployName = matchedApp.deployment;
  } else {
    const parts = podName.split('-');
    if (parts.length >= 3) {
      deployName = parts.slice(0, parts.length - 2).join('-');
    } else {
      deployName = podName;
    }
  }

  const ok = confirm(`确定要对 Deployment [${deployName}] (命名空间: ${ns}) 执行滚动重启吗？\n将执行: kubectl -n ${ns} rollout restart deployment/${deployName}`);
  if (!ok) return;

  const res = await api('POST', '/api/k8s/restart', {
    serverId: srvId,
    namespace: ns,
    deployment: deployName,
  });

  if (!res.ok) {
    alert(`重启失败: ${res.data.error || '未知错误'}`);
    return;
  }

  alert(`✔ 重启指令已下发！请稍候刷新查看 Pod 重建进度。`);
  setTimeout(loadPods, 1500);
};

// ---------------- 服务器管理 (长连接池保持) ----------------
function renderServerCards() {
  if (state.servers.length === 0) {
    $('serverCardsList').innerHTML = '<div class="config-hint">尚未配置服务器，请点击右上角添加。</div>';
    return;
  }
  $('serverCardsList').innerHTML = state.servers.map((s) => {
    const isConnected = state.activeServerIds.includes(s.id);
    const statusBadge = isConnected
      ? '<span class="pod-status-badge pod-status-running" style="margin-left:8px;">🟢 已建立长连接</span>'
      : '<span class="pod-status-badge" style="margin-left:8px;background:#f1f5f9;color:#64748b;">⚪ 未连接</span>';
    return `
      <div class="item-card">
        <div class="item-card-header">
          <span class="item-card-title">${escapeHtml(s.name)}${statusBadge}</span>
          <div style="display:flex;gap:6px;">
            <button class="btn btn-secondary btn-sm" onclick="testOneServer('${s.id}')">${isConnected ? '🔄 重新检测' : '⚡ 建立连接'}</button>
            ${isConnected ? `<button class="btn btn-secondary btn-sm" style="color:#ef4444;" onclick="disconnectOneServer('${s.id}')">🔌 断开连接</button>` : ''}
            <button class="btn btn-secondary btn-sm" onclick="editServer('${s.id}')">编辑</button>
            <button class="btn btn-danger btn-sm" onclick="deleteServer('${s.id}')">删除</button>
          </div>
        </div>
        <div class="item-card-desc">连接信息: <code>${escapeHtml(s.user)}@${escapeHtml(s.host)}:${s.port || 22}</code> (认证: ${s.authType === 'key' ? '私钥' : '密码'})</div>
        <div class="item-card-desc">连接池机制: 首次连接或发布时自动建立 SSH 长连接并保持心跳，仅在已连接状态下可手动断开释放句柄</div>
      </div>
    `;
  }).join('');
}

async function connectCurrentServer() {
  const appId = $('deployAppSelect').value;
  const app = state.apps.find((a) => a.id === appId);
  let srv = app ? state.servers.find((s) => s.id === app.serverId) : null;
  if (!srv && state.servers.length > 0) {
    srv = state.servers[0];
  }
  if (!srv) {
    return alert('尚未配置 SSH 服务器，请先在“服务器与目录配置”中添加');
  }
  const btn = $('connectSrvBtn');
  const origText = btn.textContent;
  btn.textContent = '连接中...';
  btn.disabled = true;
  const res = await api('POST', '/api/servers/test', srv);
  btn.textContent = origText;
  btn.disabled = false;
  if (res.ok) {
    await updateConnStatus();
  } else {
    alert(`✖ 连接失败: ${res.data.error || '未知错误'}`);
  }
}

window.disconnectOneServer = async function (srvId) {
  const res = await api('POST', '/api/servers/disconnect', { serverId: srvId });
  if (res.ok) {
    await updateConnStatus();
  } else {
    alert(`断开失败: ${res.data.error || '未知错误'}`);
  }
};

async function disconnectAllConnections() {
  const res = await api('POST', '/api/servers/disconnect', {});
  if (res.ok) {
    await updateConnStatus();
  }
}

async function updateConnStatus() {
  const res = await api('GET', '/api/servers/connections');
  if (res.ok) {
    const count = res.data.count || 0;
    state.activeServerIds = res.data.active || [];
    state.activeServers = res.data.servers || [];

    const dot = $('connStatusDot');
    const txt = $('connStatusText');
    const connBtn = $('connectSrvBtn');
    const discBtn = $('disconnectAllBtn');

    if (count > 0) {
      const names = state.activeServers.map((s) => `${s.name} (${s.host})`).join(', ');
      if (dot) dot.classList.add('connected');
      txt.textContent = `SSH 已连接 · ${names || count + ' 台主机'}`;
      txt.className = 'conn-text connected';
      if (connBtn) connBtn.classList.add('hidden');
      if (discBtn) discBtn.classList.remove('hidden');
    } else {
      if (dot) dot.classList.remove('connected');
      txt.textContent = 'SSH 未连接';
      txt.className = 'conn-text';
      if (connBtn) connBtn.classList.remove('hidden');
      if (discBtn) discBtn.classList.add('hidden');
    }
    renderServerCards();
  }
}

window.testOneServer = async function (srvId) {
  const srv = state.servers.find((s) => s.id === srvId);
  if (!srv) return;
  const res = await api('POST', '/api/servers/test', srv);
  if (res.ok) {
    await updateConnStatus();
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
  if ($('appCountBadge')) {
    $('appCountBadge').textContent = `${state.apps.length} 个服务`;
  }
  if (state.apps.length === 0) {
    $('appCardsList').innerHTML = '<div class="config-hint">尚未配置应用，请点击右上角添加。</div>';
    return;
  }
  const kw = ($('appConfigSearchInput') ? $('appConfigSearchInput').value : '').trim().toLowerCase();
  const filtered = kw
    ? state.apps.filter((a) =>
        (a.name || '').toLowerCase().includes(kw) ||
        (a.image || '').toLowerCase().includes(kw) ||
        (a.k8sYamlFile || '').toLowerCase().includes(kw) ||
        (a.namespace || '').toLowerCase().includes(kw)
      )
    : state.apps;

  if (filtered.length === 0) {
    $('appCardsList').innerHTML = '<div class="config-hint">未找到匹配搜索关键词的服务配置。</div>';
    return;
  }

  $('appCardsList').innerHTML = filtered.map((a) => `
    <div class="item-card">
      <div class="item-card-header">
        <span class="item-card-title">${escapeHtml(a.name)} <span class="app-type-tag">${escapeHtml(a.type)}</span></span>
        <div style="display:flex;gap:6px;">
          <button class="btn btn-secondary btn-sm" onclick="editApp('${a.id}')">编辑</button>
          <button class="btn btn-danger btn-sm" onclick="deleteApp('${a.id}')">删除</button>
        </div>
      </div>
      ${a.localBuildCmd || a.localArtifactPath ? `<div class="item-card-desc"><strong>⚡ 本地编译/默认产物:</strong> <code>${escapeHtml(a.localBuildCmd || '仅指定默认产物')}</code> (产物: <code>${escapeHtml(a.localArtifactPath || '-')}</code>)</div>` : ''}
      <div class="item-card-desc"><strong>📦 独立产物目录:</strong> <code>${escapeHtml(a.artifactDir || '-')}</code></div>
      <div class="item-card-desc"><strong>🔨 独立构建目录:</strong> <code>${escapeHtml(a.buildDir || '-')}</code></div>
      <div class="item-card-desc"><strong>📄 共享 K8s 目录 + 独立 YAML:</strong> <code>${escapeHtml(a.k8sYamlDir || '-')}</code> / <code style="color:#2563eb;font-weight:700;">${escapeHtml(a.k8sYamlFile || (a.name + '.yaml'))}</code></div>
      <div class="item-card-desc"><strong>🏷️ 独立镜像与动态 Tag:</strong> <code>${escapeHtml(a.image || '-')}:&lt;Tag&gt;</code></div>
      <div class="item-card-desc"><strong>☸️ K8s 目标工作负载:</strong> ns: <code>${escapeHtml(a.namespace)}</code>, deployment: <code>${escapeHtml(a.deployment)}</code></div>
    </div>
  `).join('');
}

function getSharedK8sYamlDir() {
  const found = state.apps.find((a) => a.k8sYamlDir);
  return found ? found.k8sYamlDir : '/data/k8s-yaml';
}

function getImagePrefix() {
  const found = state.apps.find((a) => a.image && a.image.includes('/'));
  if (found) {
    const idx = found.image.lastIndexOf('/');
    return found.image.substring(0, idx + 1);
  }
  return 'harbor.example.com/app/';
}

function autoFillAppPathsByName(forceOverwrite) {
  const name = $('editAppName').value.trim();
  if (!name) return;
  const imgPrefix = getImagePrefix();
  const sharedYamlDir = getSharedK8sYamlDir();

  if (forceOverwrite || !$('editAppImage').value.trim() || $('editAppImage').dataset.auto === '1') {
    $('editAppImage').value = `${imgPrefix}${name}`;
    $('editAppImage').dataset.auto = '1';
  }
  if (forceOverwrite || !$('editAppArtifactDir').value.trim() || $('editAppArtifactDir').dataset.auto === '1') {
    $('editAppArtifactDir').value = `/data/packages/${name}`;
    $('editAppArtifactDir').dataset.auto = '1';
  }
  if (forceOverwrite || !$('editAppBuildDir').value.trim() || $('editAppBuildDir').dataset.auto === '1') {
    $('editAppBuildDir').value = `/data/docker-build/${name}`;
    $('editAppBuildDir').dataset.auto = '1';
  }
  if (forceOverwrite || !$('editAppK8sYamlDir').value.trim()) {
    $('editAppK8sYamlDir').value = sharedYamlDir;
  }
  if (forceOverwrite || !$('editAppK8sYamlFile').value.trim() || $('editAppK8sYamlFile').dataset.auto === '1') {
    $('editAppK8sYamlFile').value = `${name}.yaml`;
    $('editAppK8sYamlFile').dataset.auto = '1';
  }
  if (forceOverwrite || !$('editAppDeployment').value.trim() || $('editAppDeployment').dataset.auto === '1') {
    $('editAppDeployment').value = name;
    $('editAppDeployment').dataset.auto = '1';
  }
  if (forceOverwrite || !$('editAppContainer').value.trim() || $('editAppContainer').dataset.auto === '1') {
    $('editAppContainer').value = name;
    $('editAppContainer').dataset.auto = '1';
  }
}

function openAppModal(app) {
  const sharedYamlDir = getSharedK8sYamlDir();
  $('editAppId').value = app ? app.id : '';
  $('editAppName').value = app ? app.name : '';
  $('editAppType').value = app ? (app.type || 'jar') : 'jar';
  $('editAppImage').value = app ? (app.image || '') : '';
  $('editAppImage').dataset.auto = app ? '0' : '1';
  $('editAppLocalProjectDir').value = app ? (app.localProjectDir || '') : '';
  $('editAppLocalBuildCmd').value = app ? (app.localBuildCmd || '') : '';
  $('editAppLocalArtifactPath').value = app ? (app.localArtifactPath || '') : '';
  $('editAppArtifactDir').value = app ? (app.artifactDir || '') : '';
  $('editAppArtifactDir').dataset.auto = app ? '0' : '1';
  $('editAppBuildDir').value = app ? (app.buildDir || '') : '';
  $('editAppBuildDir').dataset.auto = app ? '0' : '1';
  $('editAppK8sYamlDir').value = app ? (app.k8sYamlDir || sharedYamlDir) : sharedYamlDir;
  $('editAppK8sYamlFile').value = app ? (app.k8sYamlFile || (app.name ? app.name + '.yaml' : '')) : '';
  $('editAppK8sYamlFile').dataset.auto = app ? '0' : '1';
  $('editAppBuildCmd').value = app ? (app.buildCmd || '') : 'cd {{.BuildDir}} && docker build -t {{.Image}}:{{.Tag}} .';
  $('editAppPushCmd').value = app ? (app.pushCmd || '') : '';
  $('editAppDeployCmd').value = app ? (app.deployCmd || '') : 'kubectl apply -f {{.K8sYamlPath}}';
  $('editAppNamespace').value = app ? (app.namespace || '') : 'prod';
  $('editAppDeployment').value = app ? (app.deployment || '') : '';
  $('editAppDeployment').dataset.auto = app ? '0' : '1';
  $('editAppContainer').value = app ? (app.container || '') : '';
  $('editAppContainer').dataset.auto = app ? '0' : '1';

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
    localProjectDir: $('editAppLocalProjectDir').value.trim(),
    localBuildCmd: $('editAppLocalBuildCmd').value.trim(),
    localArtifactPath: $('editAppLocalArtifactPath').value.trim(),
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
  const rate = total > 0 ? Math.round((successCount / total) * 100) : 0;

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
    const emptyMsg = rawHistoryList.length === 0
      ? '暂无发布记录（完成首次构建发布后将在此展示）'
      : '未匹配到符合筛选条件的发布记录';
    $('historyTableBody').innerHTML = `<tr><td colspan="8" class="td-empty">${emptyMsg}</td></tr>`;
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
            ${isSuccess ? `
              <button class="btn btn-secondary btn-sm" style="color:#d97706;border-color:#fde68a;" title="回滚线上至此历史版本" onclick="triggerRollback('${r.id}')">
                ↩️ 回滚
              </button>
            ` : ''}
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

// ---------------- 一键回滚流程交互 (优化项 1) ----------------
let pendingRollbackRelease = null;

window.triggerRollback = function (releaseId) {
  const rel = rawHistoryList.find((r) => r.id === releaseId);
  if (!rel) return;
  pendingRollbackRelease = rel;

  $('rbAppName').textContent = rel.app || '-';
  $('rbTag').textContent = rel.tag || '-';
  $('rbImage').textContent = rel.image || '-';
  $('rollbackModal').classList.remove('hidden');
};

$('closeRollbackModalBtn').addEventListener('click', () => {
  $('rollbackModal').classList.add('hidden');
  pendingRollbackRelease = null;
});

$('rollbackCancelBtn').addEventListener('click', () => {
  $('rollbackModal').classList.add('hidden');
  pendingRollbackRelease = null;
});

$('rollbackConfirmBtn').addEventListener('click', async () => {
  if (!pendingRollbackRelease) return;
  const targetId = pendingRollbackRelease.id;
  const targetTag = pendingRollbackRelease.tag;
  const targetApp = pendingRollbackRelease.app;
  $('rollbackModal').classList.add('hidden');

  const res = await api('POST', `/api/releases/${targetId}/rollback`, {
    remark: `回滚至历史版本 [${targetTag}]`,
  });

  if (!res.ok) {
    alert(`发起回滚失败: ${res.data.error || '未知错误'}`);
    return;
  }

  const newRel = res.data;
  // 切换到控制台 TAB 1 并开启实时推流
  document.querySelector('.tab-btn[data-tab="deploy"]').click();
  $('deployAppSelect').value = targetApp;
  $('tagInput').value = targetTag;
  updatePathPreview();

  $('logConsole').textContent = '';
  $('pipelineStages').innerHTML = '';
  updateStatus('running', '回滚部署中');
  state.isDeploying = true;
  $('startDeployBtn').disabled = true;

  listenReleaseEvents(newRel.id);
});
