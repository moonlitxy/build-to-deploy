'use strict';

const $ = (id) => document.getElementById(id);
const ic = (name, cls = 'icon') => `<svg class="${cls}" aria-hidden="true"><use href="#i-${name}"></use></svg>`;
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
  // 全局 SSH 连接开关：服务启动后为 false，用户点击"连接服务器"后变 true
  connEnabled: false,
  // 镜像维护页状态：当前选中的应用与其镜像列表
  imageMaint: { appId: '', images: [] },
  // 镜像内容重复暂停时的结构化信息（来自 SSE duplicate 事件），供确认弹窗使用
  pendingDuplicate: null,
  // 各应用线上当前版本（按 appId 缓存），来自 /api/apps/{id}/current-version
  currentVersions: {},
  // 建议 Tag 只预填一次，用户手动改过之后不再覆盖
  tagTouched: false,
  pendingReleaseParams: null,
  prefs: {
    selectedAppId: '',
    deployModeByApp: {},
    useDefaultArtifactByApp: {},
    lastTagByApp: {},
    podServerId: '',
    podNamespace: '',
    configServerId: '',
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
        podServerId: parsed.podServerId || '',
        podNamespace: parsed.podNamespace || '',
        configServerId: parsed.configServerId || '',
      };
    }
  } catch (_) {}
}

function savePrefs() {
  try {
    localStorage.setItem(PREFS_KEY, JSON.stringify(state.prefs));
  } catch (_) {}
}

// 系统文件选择框同一时刻只允许一个：连点按钮不会再排队弹出多个对话框
let pathPickerPending = null;

async function pickLocalPath(mode, promptText) {
  if (pathPickerPending) {
    return pathPickerPending;
  }
  // 前端超时略长于服务端（120s），避免网络异常时按钮一直卡在"打开中..."
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 125000);
  pathPickerPending = (async () => {
    try {
      const res = await fetch('/api/local/pick-path', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode, prompt: promptText }),
        signal: controller.signal,
      });
      const data = await res.json().catch(() => ({}));
      if (res.status === 409) {
        // 已有一个选择框开着（例如另一个标签页），本次不再叠加
        return null;
      }
      if (!res.ok) {
        alert(`打开本地文件选择器失败: ${data.error || '未知错误'}`);
        return null;
      }
      if (data.canceled || !data.path) {
        return null;
      }
      return data.path;
    } catch (e) {
      if (e.name === 'AbortError') {
        alert('打开文件选择框超时，请重试');
      }
      return null;
    } finally {
      clearTimeout(timer);
      pathPickerPending = null;
    }
  })();
  return pathPickerPending;
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
  // 进入页面即拉取一次命名空间与 Pod，不必先手动刷新（仅限已连接服务器时；启动默认不连接）
  if (state.connEnabled && $('tab-pods').classList.contains('active')) {
    loadNamespacesAndPods();
  }
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
        // 进入监控页：刷新一次命名空间与 Pod，随后交给后台定时静默刷新（未连接服务器时不主动访问远程）
        if (state.connEnabled) {
          loadNamespacesAndPods();
          startPodAutoRefresh();
        }
      } else {
        stopPodAutoRefresh();
      }
      if (btn.dataset.tab === 'history') {
        loadHistory();
      }
      if (btn.dataset.tab === 'config') {
        // 镜像维护列表：只同步按钮状态，不自动访问远程
        syncDeleteImagesBtn();
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
  renderServerPicker();
  renderAppCards();
  updatePathPreview({ probeRemote: false });
  await loadAppBackups();
}

function renderAppSelects() {
  const prevVal = $('deployAppSelect').value || state.prefs.selectedAppId;
  const opts = state.apps.map((a) => `<option value="${escapeHtml(a.id)}">${escapeHtml(a.name)} (${escapeHtml(a.type)})</option>`).join('');
  $('deployAppSelect').innerHTML = opts || '<option value="">暂无应用，请先在配置中添加</option>';
  if (prevVal && state.apps.some((a) => a.id === prevVal)) {
    $('deployAppSelect').value = prevVal;
  }
  $('historyAppFilter').innerHTML = '<option value="">全部应用</option>' + opts;
  renderPodSelects();
  renderImageAppSelect();
}

// Pod 页面服务器下拉：Pod 状态由后台定时静默刷新，服务器/命名空间才是筛选条件
function renderPodSelects() {
  const srvOpts = state.servers.map((s) => `<option value="${escapeHtml(s.id)}">${escapeHtml(s.name)} (${escapeHtml(s.host)})</option>`).join('');
  $('podServerSelect').innerHTML = srvOpts || '<option value="">暂无服务器</option>';
  const savedSrv = state.prefs.podServerId;
  if (savedSrv && state.servers.some((s) => s.id === savedSrv)) {
    $('podServerSelect').value = savedSrv;
  } else if (state.servers.length > 0) {
    $('podServerSelect').value = state.servers[0].id;
  }
}

// 查询容器形态下宿主机上同名容器的状态（运行中 / 已停止 / 不存在）
async function refreshContainerStatus(appId) {
  const el = $('pvDocker');
  if (!el) return;
  // 未连接服务器时不探测远程容器状态：启动/刷新页面要保持纯本地
  if (!state.connEnabled) {
    el.innerHTML += ' <span style="color:var(--text-muted)">（未连接服务器，不查询状态）</span>';
    return;
  }
  const res = await api('GET', `/api/deploy-target?appId=${encodeURIComponent(appId)}`);
  if (!res.ok || !res.data || res.data.deployMode !== 'docker') return;
  const st = res.data.status;
  if (st === 'running') {
    el.innerHTML += ` <span style="color:var(--success-text)">${ic('check', 'icon icon-inline')} 运行中${res.data.image ? ' · ' + escapeHtml(res.data.image) : ''}</span>`;
  } else if (st) {
    el.innerHTML += ` <span style="color:var(--warning-text)">${ic('alert', 'icon icon-inline')} ${escapeHtml(st)}</span>`;
  } else if (!res.data.error) {
    el.innerHTML += ' <span style="color:var(--text-muted)">（容器不存在，发布时创建）</span>';
  }
}

function updatePathPreview({ probeRemote = true } = {}) {
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
  // 展示线上当前版本：只有已连接服务器时才查询，避免启动/切页时访问远程
  renderCurrentVersion(appId);
  if (probeRemote && state.connEnabled) refreshCurrentVersion(appId);
  const srv = state.servers.find((s) => s.id === app.serverId);
  $('pvServer').textContent = srv ? `${srv.name} (${srv.user}@${srv.host})` : (app.serverId || '-');
  $('pvArtifactDir').textContent = app.artifactDir || '未配置';
  $('pvBuildDir').textContent = app.buildDir || '未配置';
  const isDocker = app.deployMode === 'docker';
  if (isDocker) {
    $('pvYamlDir').textContent = 'Docker 容器形态（不依赖 K8s）';
  } else if (app.k8sYamlDir && app.k8sYamlFile) {
    $('pvYamlDir').innerHTML = `<span>${escapeHtml(app.k8sYamlDir)}/</span><strong style="color:var(--primary)">${escapeHtml(app.k8sYamlFile)}</strong>`;
  } else {
    $('pvYamlDir').textContent = app.k8sYamlFile || app.k8sYamlDir || '未配置';
  }
  // Docker 形态：展示容器名与端口映射，并实时查询宿主机上的容器状态
  const dockerItem = $('pvDockerItem');
  if (dockerItem) {
    dockerItem.classList.toggle('hidden', !isDocker);
    if (isDocker) {
      $('pvDocker').textContent = `${app.dockerContainer || app.deployment || app.name}${app.dockerPort ? ' · ' + app.dockerPort : ''}`;
      // 只有用户主动切换应用时才顺便查一次远程容器状态；页面初始化/刷新不碰远程
      if (probeRemote) refreshContainerStatus(app.id);
    }
  }
  const curTag = $('tagInput').value.trim() || '<Tag>';
  $('pvImage').innerHTML = app.image
    ? `<span>${escapeHtml(app.image)}:</span><strong style="color:var(--success-text)">${escapeHtml(curTag)}</strong>`
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
    pathEl.textContent = '可点击右侧“选择文件”选取本地产物并自动记住该路径';
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
    badgeEl.textContent = '本地暂未生成此文件';
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
    startBtn.innerHTML = `${ic('bolt')} 执行本地编译并上传部署`;
  } else {
    startBtn.classList.remove('mode-local-build');
    startBtn.innerHTML = `${ic('arrow-up')} 开始上传并构建部署`;
  }
}

// 本地产物路径直传与手动选择文件是互斥的：勾选直传后拖拽/点击选择不再生效
function defaultArtifactInUse() {
  const chk = $('useDefaultArtifactCheck');
  return !!(chk && chk.checked && state.localArtifactInfo && state.localArtifactInfo.exists);
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
    $('dropText').textContent = '已勾选「使用本地产物路径直传」，光标/拖拽已禁用；如需手动选择文件请先取消上方勾选';
  } else {
    dropZone.classList.remove('has-file');
    dropZone.classList.remove('disabled-zone');
    $('dropText').textContent = '点击选择 或 将编译好的产物拖拽至此处';
  }
}

$('deployAppSelect').addEventListener('change', () => {
  // 切换应用是新的填写场景：允许按新应用的线上版本再次预填建议 Tag
  state.tagTouched = false;
  $('tagInput').value = '';
  updatePathPreview();
});

// ---------------- 本地文件拖拽与选择 ----------------
function setupDropZone() {
  const dropZone = $('dropZone');
  const fileInput = $('fileInput');

  dropZone.addEventListener('click', () => {
    // 互斥：已勾选本地产物路径直传时，不允许再手动选文件
    if (defaultArtifactInUse()) return;
    fileInput.click();
  });
  fileInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
      handleSelectedFile(e.target.files[0]);
    }
  });

  dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    if (defaultArtifactInUse()) return;
    dropZone.classList.add('dragover');
  });
  dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
  dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    if (defaultArtifactInUse()) return;
    if (e.dataTransfer.files.length > 0) {
      handleSelectedFile(e.dataTransfer.files[0]);
    }
  });
}

function handleSelectedFile(file) {
  state.selectedFile = file;
  state.uploadId = ''; // 换文件后旧上传任务失效，预检不能再用上一次的分片摘要
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

// ---------------- 线上当前版本与 Tag 建议 ----------------
async function refreshCurrentVersion(appId) {
  try {
    const res = await api('GET', `/api/apps/${encodeURIComponent(appId)}/current-version`);
    if (res.ok && res.data) {
      state.currentVersions[appId] = { tag: res.data.tag || '', image: res.data.image || '', deployedAt: res.data.deployedAt || '', error: '' };
    } else {
      state.currentVersions[appId] = { tag: '', image: '', error: res.data && res.data.error ? res.data.error : '读取失败' };
    }
  } catch (e) {
    state.currentVersions[appId] = { tag: '', image: '', error: '读取失败' };
  }
  if ($('deployAppSelect').value === appId) {
    renderCurrentVersion(appId);
    maybePrefillTag(appId);
  }
}

function suggestionFor(tag) {
  // 只对纯数字点分版本给建议（v1.2.3 → v1.2.4），其它命名交给用户
  const m = /^(v?)(\d+)\.(\d+)\.(\d+)$/.exec(tag || '');
  if (!m) return '';
  return `${m[1]}${m[2]}.${m[3]}.${Number(m[4]) + 1}`;
}

function renderCurrentVersion(appId) {
  const el = $('currentVersionLine');
  if (!el) return;
  if (!state.connEnabled) {
    el.className = 'current-version-line is-muted';
    el.textContent = '线上当前版本：连接服务器后可查看';
    return;
  }
  const v = state.currentVersions[appId];
  if (!v) {
    el.className = 'current-version-line is-muted';
    el.textContent = '线上当前版本：读取中...';
    return;
  }
  if (v.error) {
    el.className = 'current-version-line is-muted';
    el.textContent = `线上当前版本：暂时无法读取（${v.error}）`;
    renderTagRelation();
    return;
  }
  if (!v.tag) {
    el.className = 'current-version-line is-muted';
    el.textContent = '线上当前版本：该服务尚未部署过';
    renderTagRelation();
    return;
  }
  const when = v.deployedAt ? ` · 上线于 ${fmtTime(v.deployedAt)}` : '';
  el.className = 'current-version-line';
  el.textContent = `线上当前版本：${v.tag}${when}`;
  const sug = suggestionFor(v.tag);
  if (sug && !state.tagTouched && !$('tagInput').value.trim()) {
    el.textContent += ` · 建议填写 ${sug}`;
  }
  renderTagRelation();
}

function maybePrefillTag(appId) {
  const v = state.currentVersions[appId];
  if (!v || v.error || state.tagTouched || $('tagInput').value.trim()) return;
  const sug = suggestionFor(v.tag);
  if (sug) {
    $('tagInput').value = sug;
    renderCurrentVersion(appId);
  }
}

// renderTagRelation 提示当前填写的 Tag 与线上版本的关系（只提示，不阻断）
function renderTagRelation() {
  const appId = $('deployAppSelect').value;
  const v = state.currentVersions[appId];
  const tag = $('tagInput').value.trim();
  const tip = $('tagRelationLine');
  if (!tip) return;
  tip.className = 'tag-relation-line';
  tip.textContent = '';
  if (!v || v.error || !v.tag || !tag) return;
  if (tag === v.tag) {
    tip.className = 'tag-relation-line is-same';
    tip.textContent = `与线上当前版本相同：线上镜像不会变化，发布时会弹窗确认`;
    return;
  }
  const cmp = compareVersion(tag, v.tag);
  if (cmp === 0) return;
  if (cmp > 0) {
    tip.className = 'tag-relation-line is-newer';
    tip.textContent = `新版本（线上为 ${v.tag}）`;
  } else {
    tip.className = 'tag-relation-line is-older';
    tip.textContent = `低于线上当前版本 ${v.tag}：相当于回退，请确认是否符合预期`;
  }
}

function compareVersion(a, b) {
  const pa = String(a).replace(/^v/i, '').split(/[.\-+_]/);
  const pb = String(b).replace(/^v/i, '').split(/[.\-+_]/);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const x = pa[i] === undefined ? '' : pa[i];
    const y = pb[i] === undefined ? '' : pb[i];
    const nx = Number(x), ny = Number(y);
    if (!Number.isNaN(nx) && !Number.isNaN(ny) && x !== '' && y !== '') {
      if (nx !== ny) return nx > ny ? 1 : -1;
    } else if (x !== y) {
      return x > y ? 1 : -1;
    }
  }
  return 0;
}

// ---------------- 发布主流程 ----------------
function setupButtons() {
  $('startDeployBtn').addEventListener('click', onStartDeployClick);
  $('conflictConfirmBtn').addEventListener('click', () => {
    // 同一个弹窗承载两种场景：版本冲突（覆盖/复用）与镜像内容重复（仍在部署中确认继续）
    if ($('conflictModal').dataset.mode === 'duplicate') {
      onConfirmDuplicateDeploy();
      return;
    }
    onConfirmConflictDeploy();
  });
  $('conflictRebuildBtn').addEventListener('click', onRebuildInsteadOfReuse);
  $('conflictCancelBtn').addEventListener('click', async () => {
    const modal = $('conflictModal');
    const isDuplicate = modal.dataset.mode === 'duplicate';
    const releaseId = modal.dataset.releaseId;
    modal.classList.add('hidden');
    // 镜像重复暂停的记录不在"运行中"，不主动放弃会永远停在待确认
    if (isDuplicate && releaseId) {
      const res = await api('POST', `/api/releases/${encodeURIComponent(releaseId)}/cancel`);
      if (!res.ok) {
        // 已经在跑（例如另一处确认了继续）时取消接口会报"未在运行"，此时保留原记录即可
        appendLog(`[提示] 放弃本次发布未生效: ${res.data.error || res.status}`);
      } else {
        appendLog('[提示] 已放弃本次发布（镜像内容与线上相同，未部署）');
        updateStatus('idle', '已放弃');
      }
    }
    modal.dataset.mode = '';
    modal.dataset.releaseId = '';
  });
  $('clearLogBtn').addEventListener('click', () => { $('logConsole').textContent = ''; });
  $('refreshPodsBtn').addEventListener('click', refreshPodsManually);
  $('podServerSelect').addEventListener('change', () => {
    // 记住 Pod 监控页最后查看的服务器；切换服务器需要重新拉取命名空间
    state.prefs.podServerId = $('podServerSelect').value;
    savePrefs();
    loadNamespacesAndPods(true);
  });
  $('podNamespaceSelect').addEventListener('change', () => {
    // 记住最后查看的命名空间
    state.prefs.podNamespace = $('podNamespaceSelect').value;
    savePrefs();
    loadPods();
  });
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
        state.uploadId = '';
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

  // 配置页：服务器下拉与操作按钮（只展示所选服务器的配置）
  if ($('configServerSelect')) {
    $('configServerSelect').addEventListener('change', () => {
      state.prefs.configServerId = $('configServerSelect').value;
      savePrefs();
      renderServerPicker();
      renderAppCards();
    });
  }
  if ($('configEditServerBtn')) {
    $('configEditServerBtn').addEventListener('click', () => {
      if ($('configServerSelect').value) window.editServer($('configServerSelect').value);
    });
  }
  if ($('configDeleteServerBtn')) {
    $('configDeleteServerBtn').addEventListener('click', () => {
      if ($('configServerSelect').value) window.deleteServer($('configServerSelect').value);
    });
  }
  if ($('configConnectBtn')) {
    $('configConnectBtn').addEventListener('click', () => {
      const id = $('configServerSelect').value;
      if (id) window.testOneServer(id);
    });
  }
  if ($('configDisconnectBtn')) {
    $('configDisconnectBtn').addEventListener('click', () => {
      const id = $('configServerSelect').value;
      if (id) window.disconnectOneServer(id);
    });
  }

  // 配置相关模态框事件
  $('addServerBtn').addEventListener('click', () => openServerModal());
  $('closeServerModalBtn').addEventListener('click', () => $('serverModal').classList.add('hidden'));
  $('saveServerBtn').addEventListener('click', saveServer);
  $('testServerConnBtn').addEventListener('click', testServerConnection);
  bindServerPasswordToggle();
  if ($('refreshBackupsBtn')) $('refreshBackupsBtn').addEventListener('click', loadAppBackups);
  $('editServerAuthType').addEventListener('change', (e) => {
    $('pwdGroup').classList.toggle('hidden', e.target.value === 'key');
    $('keyGroup').classList.toggle('hidden', e.target.value !== 'key');
  });

  $('addAppBtn').addEventListener('click', () => openAppModal());
  $('closeAppModalBtn').addEventListener('click', () => {
    stashAppDraft();
    $('appModal').classList.add('hidden');
  });
  $('saveAppBtn').addEventListener('click', saveApp);
  if ($('editAppDeployMode')) $('editAppDeployMode').addEventListener('change', toggleDockerConfigBox);
  if ($('editAppDockerContainer')) {
    $('editAppDockerContainer').addEventListener('input', (e) => {
      e.target.dataset.manual = '1';
    });
  }
  if ($('connectSrvBtn')) {
    $('connectSrvBtn').addEventListener('click', connectCurrentServer);
  }
  if ($('refreshImagesBtn')) {
    $('refreshImagesBtn').addEventListener('click', loadImages);
  }
  if ($('deleteImagesBtn')) {
    $('deleteImagesBtn').addEventListener('click', deleteSelectedImages);
  }
  if ($('imageMaintList')) {
    $('imageMaintList').addEventListener('change', (e) => {
      if (e.target.matches('input[data-image-ref]')) syncDeleteImagesBtn();
    });
  }
  if ($('imageMaintAppSelect')) {
    $('imageMaintAppSelect').addEventListener('change', () => {
      state.imageMaint = { appId: $('imageMaintAppSelect').value, images: [] };
      renderImages();
    });
  }
  $('disconnectAllBtn').addEventListener('click', disconnectAllConnections);
  $('tagInput').addEventListener('input', () => {
    state.tagTouched = true;
    renderTagRelation();
    const appId = $('deployAppSelect').value;
    const app = state.apps.find((a) => a.id === appId);
    if (app && app.image) {
      const curTag = $('tagInput').value.trim() || '<Tag>';
      $('pvImage').innerHTML = `<span>${escapeHtml(app.image)}:</span><strong style="color:var(--success-text)">${escapeHtml(curTag)}</strong>`;
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
      syncDockerContainerFromName();
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
      return alert('请先选择本地构建产物文件，或切换至“本地一键编译并上传”模式');
    }
  }

  if (!tag || !TAG_RE.test(tag)) return alert('请输入合法的镜像 Tag (只能包含字母、数字、点、减号、下划线)');

  appendLog(`\n[本地] 准备发布应用: ${appId}, Tag: ${tag} (${isLocalBuild ? '本地一键编译并上传' : '直接上传已有产物'})`);
  appendLog('[本地] 正在向服务器执行版本预检 (Precheck)...');

  // 1. 构建输入指纹预检：产物内容 + 构建上下文与历史版本都相同时，换 Tag 也只会得到同一个镜像。
  // 手动选择的文件此时还没上传、浏览器拿不到真实路径，因此等上传完成后再按分片摘要校验（见 startDeployPipeline）
  if (!state.selectedFile) {
    appendLog('[本地] 正在比对产物内容与构建上下文是否与历史版本相同...');
    const draft = state.appDrafts && state.appDrafts[appId];
    const fpRes = await api('POST', '/api/artifacts/inspect', {
      appId,
      tag,
      uploadId: state.uploadId || '',
      useLocalArtifact: !!useDefaultArtifact,
      buildDir: (draft && draft.buildDir) || '',
      buildCmd: (draft && draft.buildCmd) || '',
    });
    if (fpRes.ok && fpRes.data && fpRes.data.fingerprint) {
      const prev = fpRes.data.previous;
      if (prev || fpRes.data.tagFree) {
        appendLog(`[警告] 本次产物与构建上下文和${prev ? '已验证版本 ' + prev.tag : '历史版本'}完全相同，换 Tag 只会构建出内容相同的镜像`);
        showImageDupModal({
          tag,
          image: app.image,
          prevTag: prev ? prev.tag : '',
          prevImage: prev ? prev.image : '',
          createdAt: prev ? prev.createdAt : '',
          tagFree: !!fpRes.data.tagFree,
          sha256: fpRes.data.sha256 || '',
        });
        state.pendingReleaseParams = { appId, tag, remark, isLocalBuild, useDefaultArtifact, uploadId: '' };
        return;
      }
      appendLog(`[本地] 构建输入指纹: ${fpRes.data.fingerprint.slice(0, 12)}，与历史版本均不相同`);
    } else if (fpRes.ok === false) {
      appendLog(`[提示] 构建输入指纹比对跳过: ${fpRes.data.error || '无法读取产物'}`);
    } else {
      appendLog(`[提示] 已跳过发布前镜像内容检测: ${fpRes.data.reason || '构建命令无法静态解析'}（构建完成后仍会比对实际镜像摘要）`);
    }
  } else {
    appendLog('[本地] 产物尚未上传：上传完成后将用产物 SHA256 与构建上下文做镜像内容校验');
  }

  // 2. 版本冲突预检 (需求 2 核心要求)
  const preRes = await api('POST', '/api/precheck', { appId, tag });
  if (!preRes.ok) {
    appendLog(`[预检失败] ${preRes.data.error || '检查失败'}`);
    return alert(`预检失败: ${preRes.data.error || '未知错误'}`);
  }

  (preRes.data.warnings || []).forEach((w) => appendLog(`[提示] ${w}`));
  const conflicts = preRes.data.conflicts || [];
  if (conflicts.length > 0) {
    // 发现同版本 Tag 或线上/已部署过的同内容镜像，弹出确认弹窗
    state.pendingReleaseParams = { appId, tag, remark, isLocalBuild, useDefaultArtifact, uploadId: '' };
    showConflictModal(conflicts, tag);
    return;
  }

  // 无冲突，直接开始
  startDeployPipeline(appId, tag, remark, false, isLocalBuild, useDefaultArtifact);
}

// showConflictModal 展示预检冲突并让用户确认；k8s-image（线上内容重复）与会丢弃线上版本的覆盖都不是"复用"
function showConflictModal(conflicts, tag) {
  if ($('conflictIntro')) $('conflictIntro').textContent = '检测到目标服务器或线上环境已存在与当前完全相同的镜像 Tag 版本：';
  const itemsHtml = conflicts.map((c) => `<div class="conflict-item">[${c.source}] ${escapeHtml(c.message)}</div>`).join('');
  $('conflictItems').innerHTML = itemsHtml;
  // 仅"服务器上本地已有同内容镜像"（docker-image）会走复用；线上正在跑的同内容镜像（k8s-image）不能当成复用
  const reusable = conflicts.length > 0 && conflicts.every((c) => c.source === 'docker-image');
  const foot = $('conflictFoot');
  if (foot) {
    foot.innerHTML = reusable
      ? '继续发布将<strong>直接复用内容相同的已有镜像</strong>（只追加新 Tag），不会重新构建，线上镜像内容不会变化。'
      : '继续发布将<strong>覆盖构建该镜像</strong>并执行 K8s 滚动更新。请确认是否强制覆盖？';
  }
  setModalTone(reusable ? 'notice' : 'danger');
  setConflictActions({
    title: reusable ? '检测到镜像内容重复' : '发现版本冲突 - 需二次确认',
    confirmText: reusable ? '复用已有镜像并添加新 Tag' : '确认覆盖并继续发布',
    showRebuild: reusable,
  });
  // 服务端真实模式下会直接拒绝同指纹发布；前端在覆盖确认后仍以 force=true 提交，由服务端决定是否放行
  $('conflictModal').classList.remove('hidden');
}

// setModalTone 设置弹窗语气：danger（覆盖/冲突，红色）或 notice（重复提醒，琥珀色）
function setModalTone(tone) {
  const card = document.querySelector('#conflictModal .modal-card');
  const badge = document.querySelector('#conflictModal .modal-icon-badge');
  [card, badge].forEach((el) => {
    if (!el) return;
    el.classList.remove('is-danger', 'is-notice');
    el.classList.add(tone === 'danger' ? 'is-danger' : 'is-notice');
  });
  const confirmBtn = $('conflictConfirmBtn');
  if (confirmBtn) confirmBtn.classList.toggle('btn-danger', tone === 'danger');
  if (confirmBtn) confirmBtn.classList.toggle('btn-primary', tone !== 'danger');
}

// setConflictActions 统一控制冲突弹窗的标题与按钮（需求的"是否把新 Tag 指向已有镜像"）
function setConflictActions(opts) {
  const title = $('conflictModalTitle');
  if (title && opts.title) title.textContent = opts.title;
  const confirmBtn = $('conflictConfirmBtn');
  if (confirmBtn) confirmBtn.textContent = opts.confirmText || '确认覆盖并继续发布';
  const rebuildBtn = $('conflictRebuildBtn');
  if (rebuildBtn) rebuildBtn.classList.toggle('hidden', !opts.showRebuild);
}

// 上传前的"同镜像不同 Tag"提醒：产物哈希与历史版本相同，换 Tag 不会得到新镜像。
// 明确给出哈希校验结果，并让用户决定"复用已有镜像"还是"仍然重新构建"。
function showImageDupModal(info) {
  const img = (t) => (info.image ? `${escapeHtml(info.image)}:${escapeHtml(t)}` : escapeHtml(t));
  const sha = info.sha256 ? escapeHtml(info.sha256) : '';
  $('conflictItems').innerHTML = `
    <div class="conflict-item">🔎 产物 SHA256 校验：<code>${sha ? sha.slice(0, 24) + '…' : '未计算出摘要'}</code>（${sha ? '完整摘要与前次一致' : '无法比对'}）</div>
    <div class="conflict-item">[fingerprint] 本次产物内容与构建上下文（Dockerfile 等）和${info.prevTag ? '已验证版本 ' + escapeHtml(info.prevTag) : '历史版本'}完全相同</div>
    <div class="conflict-item">将要创建的版本：${img(info.tag)}${info.prevTag ? '（与 ' + img(info.prevTag) + (info.createdAt ? '，' + fmtTime(info.createdAt) : '') + ' 内容一致）' : ''}</div>
    <div class="conflict-item">🧩 构建命令${info.tagFree ? '不引用 Tag' : '中产物与上下文均未变化'}，重建只会得到同一个镜像</div>`;
  const foot = $('conflictFoot');
  if (foot) {
    foot.innerHTML = '这通常意味着产物没有重新编译，或只是改了个文件名/换了 Tag。请选择：<strong>复用已有镜像</strong>（只把新 Tag 指向它，最快）或<strong>仍然重新构建</strong>（构建出的内容仍会相同）。';
  }
  setModalTone('notice');
  setConflictActions({
    title: '镜像内容重复 - 选择如何处理新 Tag',
    confirmText: '复用已有镜像并添加新 Tag',
    showRebuild: true,
  });
  $('conflictModal').classList.remove('hidden');
}

async function onConfirmConflictDeploy() {
  $('conflictModal').classList.add('hidden');
  if (!state.pendingReleaseParams) return;
  const { appId, tag, remark, isLocalBuild, useDefaultArtifact, uploadId } = state.pendingReleaseParams;
  appendLog(`[确认覆盖] 用户确认：复用内容相同的已有镜像，仅把新 Tag [${tag}] 指向它`);
  startDeployPipeline(appId, tag, remark, true, isLocalBuild, useDefaultArtifact, false, uploadId || '', true);
}

// 用户选择"仍然重新构建"：不复用已有镜像，强制执行 docker build（内容相同的提示仍会保留在日志里）
async function onRebuildInsteadOfReuse() {
  $('conflictModal').classList.add('hidden');
  if (!state.pendingReleaseParams) return;
  const { appId, tag, remark, isLocalBuild, useDefaultArtifact, uploadId } = state.pendingReleaseParams;
  appendLog(`[确认覆盖] 用户选择重新构建镜像（不复用已有镜像），版本: ${tag}`);
  startDeployPipeline(appId, tag, remark, true, isLocalBuild, useDefaultArtifact, true, uploadId || '', true);
}

// 镜像内容重复（以镜像摘要为准）：线上不会有变化，由用户决定是否仍然部署
function showDuplicateDeployModal(info, releaseId) {
  if ($('conflictIntro')) $('conflictIntro').textContent = '部署前按镜像内容摘要（Docker image digest）比对，检测到重复：';
  const short = (v) => (v ? escapeHtml(String(v).slice(0, 19)) + '…' : '-');
  $('conflictItems').innerHTML = `
    <div class="conflict-item">🧱 本次镜像摘要：<code>${short(info.imageSummary)}</code></div>
    ${info.sameAsCurrent
      ? `<div class="conflict-item">线上正在运行的版本：<strong>${escapeHtml(info.currentTag || '-')}</strong>（摘要 <code>${short(info.currentImage)}</code>），与本次镜像<strong>完全相同</strong></div>
         <div class="conflict-item">⚙️ 部署后线上镜像不会有任何变化，服务内容与现在一致</div>`
      : `<div class="conflict-item">历史版本 <strong>${escapeHtml(info.duplicateTag || '-')}</strong> 与本次镜像内容完全相同，只是换了个 Tag</div>`}`;
  const foot = $('conflictFoot');
  if (foot) {
    foot.innerHTML = '这通常意味着产物没有重新编译、Dockerfile 与基础镜像都没变。仍然继续将<strong>按当前镜像部署一次</strong>，线上内容不会变化。';
  }
  setModalTone('notice');
  setConflictActions({
    title: '镜像内容与线上相同 - 确认是否继续部署',
    confirmText: '仍然部署（内容不变）',
    showRebuild: false,
  });
  // 复用"取消"按钮：直接放弃，发布记录停在待确认状态
  $('conflictModal').dataset.mode = 'duplicate';
  $('conflictModal').dataset.releaseId = releaseId || '';
  $('conflictModal').dataset.imageSummary = info.imageSummary || '';
  $('conflictModal').classList.remove('hidden');
  state.isDeploying = false;
  $('startDeployBtn').disabled = false;
  $('cancelReleaseBtn').classList.add('hidden');
}

async function onConfirmDuplicateDeploy() {
  const modal = $('conflictModal');
  const releaseId = modal.dataset.releaseId;
  const imageSummary = modal.dataset.imageSummary;
  modal.classList.add('hidden');
  modal.dataset.mode = '';
  if (!releaseId) {
    alert('发布记录已丢失，请重新发布');
    return;
  }
  appendLog(`[确认] 用户确认镜像内容未变化，仍继续部署: ${releaseId}`);
  state.isDeploying = true;
  $('startDeployBtn').disabled = true;
  $('cancelReleaseBtn').classList.remove('hidden');
  const res = await api('POST', `/api/releases/${encodeURIComponent(releaseId)}/resume`, { imageSummary });
  if (!res.ok) {
    if (res.status === 409 && res.data && res.data.duplicate) {
      // 确认期间线上又变了：用新的摘要重新弹窗
      showDuplicateDeployModal(res.data.duplicate, releaseId);
      return;
    }
    alert(`继续部署失败: ${res.data.error || res.status}`);
    updateStatus('failed', '继续部署失败');
    resetDeployUi();
    return;
  }
  state.releaseId = releaseId;
  updateStatus('running', '已确认继续部署');
  listenReleaseEvents(releaseId);
}

// resetDeployUi 让发布按钮恢复可点击（预检/校验弹窗返回或异常时）
function resetDeployUi() {
  state.isDeploying = false;
  $('startDeployBtn').disabled = false;
  $('cancelReleaseBtn').classList.add('hidden');
}

async function startDeployPipeline(appId, tag, remark, force, isLocalBuild, useDefaultArtifact, noReuse = false, existingUploadId = '', skipDupCheck = false) {
  state.pendingDuplicate = null;
  const draft = state.appDrafts && state.appDrafts[appId];
  state.isDeploying = true;
  $('startDeployBtn').disabled = true;
  $('cancelReleaseBtn').classList.remove('hidden');
  updateStatus('running', isLocalBuild ? '正在执行本地编译...' : '正在准备产物...');

  try {
    let upId = existingUploadId || '';
    if (!isLocalBuild && !upId && state.selectedFile) {
      // 2. 手动选择的本地文件分片上传至本地暂存区
      updateStatus('running', '正在上传产物...');
      upId = await uploadFileChunks(state.selectedFile);
    }
    state.uploadId = upId;

    // 2.5 上传完成后再做一次产物哈希/构建输入指纹校验（手动选择的文件此前拿不到路径）。
    // 与历史版本完全相同时提示用户：是复用已有镜像（新 Tag 指向它），还是仍然重新构建。
    if (upId && !isLocalBuild && !skipDupCheck) {
      const app = state.apps.find((a) => a.id === appId);
      const dupRes = await api('POST', '/api/artifacts/inspect', { appId, tag, uploadId: upId });
      if (dupRes.ok && dupRes.data && dupRes.data.previous) {
        const prev = dupRes.data.previous;
        appendLog(`[警告] 产物 SHA256 ${String(dupRes.data.sha256 || '').slice(0, 12)} 与已验证版本 ${prev.tag} 完全相同，镜像内容不会变化`);
        showImageDupModal({
          tag,
          image: app ? app.image : '',
          prevTag: prev.tag,
          prevImage: prev.image,
          createdAt: prev.createdAt,
          tagFree: !!dupRes.data.tagFree,
          sha256: dupRes.data.sha256 || '',
        });
        state.pendingReleaseParams = { appId, tag, remark, isLocalBuild, useDefaultArtifact, uploadId: upId };
        resetDeployUi();
        updateStatus('idle', '等待确认');
        return;
      }
      if (dupRes.ok && dupRes.data && dupRes.data.sha256 && !dupRes.data.fingerprint) {
        appendLog(`[本地] 产物 SHA256: ${dupRes.data.sha256.slice(0, 16)}（构建命令无法静态解析，跳过构建输入比对）`);
      }
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
      noReuse: !!noReuse,
      remark,
      buildDir: (draft && draft.buildDir) || '',
      buildCmd: (draft && draft.buildCmd) || '',
    });
    state.uploadId = ''; // 已交给发布流水线，避免下次预检误用

    if (startRes.status === 409 && startRes.data && startRes.data.duplicate) {
      // 构建出的镜像与线上/历史版本内容完全相同：流水线已暂停，等用户在弹窗里决定
      showDuplicateDeployModal(startRes.data.duplicate, startRes.data.id || state.releaseId || '');
      return;
    }
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
    resetDeployUi();
  }
}

// ---------------- 分片上传 ----------------
// 断点续传：同名、同大小、同修改时间的本地文件视为同一任务，
// 中断（网络失败、刷新页面、发布重试）后从服务端 received 处继续，不必从头传。
const UPLOAD_TASK_KEY = 'btd_upload_task';

async function putChunkWithRetry(uploadId, offset, chunk, tries = 3) {
  let lastErr = null;
  for (let i = 0; i < tries; i++) {
    try {
      return await fetch(`/api/uploads/${encodeURIComponent(uploadId)}?offset=${offset}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/octet-stream' },
        body: chunk,
      });
    } catch (e) {
      lastErr = e;
      await new Promise((r) => setTimeout(r, 500 * (i + 1)));
    }
  }
  throw lastErr;
}

async function uploadFileChunks(file) {
  $('uploadProgressContainer').classList.remove('hidden');
  $('uploadProgressBar').style.width = '0%';

  const fp = `${file.name}|${file.size}|${file.lastModified}`;
  let uploadId = '';
  let offset = 0;
  try {
    const saved = JSON.parse(localStorage.getItem(UPLOAD_TASK_KEY) || 'null');
    if (saved && saved.fp === fp && saved.uploadId) {
      const st = await api('GET', `/api/uploads/${encodeURIComponent(saved.uploadId)}`);
      if (st.ok && st.data.fileName === file.name && st.data.received <= file.size) {
        uploadId = saved.uploadId;
        offset = st.data.received || 0;
        if (offset > 0) appendLog(`[续传] 检测到未完成的分片任务，从 ${fmtSize(offset)} 处继续上传...`);
      }
    }
  } catch (_) { /* 续传查询失败则全新上传 */ }

  if (!uploadId) {
    const createRes = await api('POST', '/api/uploads', {
      fileName: file.name,
      size: file.size,
    });
    if (!createRes.ok) throw new Error(createRes.data.error || '初始化上传任务失败');
    uploadId = createRes.data.id;
    try {
      localStorage.setItem(UPLOAD_TASK_KEY, JSON.stringify({ fp, uploadId }));
    } catch (_) {}
  }

  const startTime = Date.now();
  let stall = 0;

  while (offset < file.size) {
    const chunk = file.slice(offset, offset + CHUNK_SIZE);
    const chunkRes = await putChunkWithRetry(uploadId, offset, chunk);

    if (!chunkRes.ok) {
      const errJson = await chunkRes.json().catch(() => ({}));
      // 409 表示服务端已接收更多（此前超时的分片实际已落地）：同步进度后继续
      if (chunkRes.status === 409 && typeof errJson.received === 'number') {
        offset = errJson.received;
        stall = 0;
        continue;
      }
      throw new Error(errJson.error || `分片上传失败 HTTP ${chunkRes.status}`);
    }

    const resData = await chunkRes.json();
    if (typeof resData.received !== 'number' || resData.received <= offset) {
      if (++stall >= 5) throw new Error('上传长时间无进展，已中止（重试可从断点继续）');
      await new Promise((r) => setTimeout(r, 500));
      continue;
    }
    stall = 0;
    offset = resData.received;

    const pct = Math.min(100, Math.round((offset / file.size) * 100));
    $('uploadProgressBar').style.width = pct + '%';
    const elapsedSec = (Date.now() - startTime) / 1000;
    const speed = elapsedSec > 0 ? (offset / elapsedSec) : 0;
    $('uploadProgressInfo').textContent = `${pct}% (${fmtSize(offset)} / ${fmtSize(file.size)})`;
    $('uploadSpeedInfo').textContent = `${fmtSize(speed)}/s`;
  }

  const completeRes = await api('POST', `/api/uploads/${encodeURIComponent(uploadId)}/complete`);
  if (!completeRes.ok) throw new Error(completeRes.data.error || '完成上传校验失败');

  try {
    localStorage.removeItem(UPLOAD_TASK_KEY);
  } catch (_) {}
  appendLog(`✔ 本地分片上传完毕，SHA256: ${completeRes.data.sha256}`);
  return uploadId;
}

// ---------------- SSE 实时日志与阶段跟踪 ----------------
function listenReleaseEvents(releaseId, isReconnect = false) {
  if (state.eventSource) {
    state.eventSource.close();
  }
  // 重连时后端会重放最多 5000 行历史日志，必须先清空面板，否则日志会整段重复
  if (isReconnect) {
    $('logConsole').textContent = '';
    appendLog('[系统] 连接已恢复，正在回放本次发布日志...');
  }

  const es = new EventSource(`/api/releases/${releaseId}/events`);
  state.eventSource = es;

  es.addEventListener('release', (e) => {
    const rel = JSON.parse(e.data);
    renderStages(rel.stages || []);
    const finished = ['success', 'failed', 'canceled', 'rolledback', 'needs_confirm'].includes(rel.status);
    if (rel.status === 'needs_confirm') {
      // 流水线在部署前检测到镜像内容重复并已暂停：弹出确认框，确认后调 /resume 继续。
      // 优先等 duplicate 事件带来的完整信息；没有时按错误文案尽力还原
      updateStatus('idle', '等待确认是否仍要部署');
      const info = state.pendingDuplicate && state.pendingDuplicate.releaseId === rel.id
        ? state.pendingDuplicate
        : {
            imageSummary: rel.imageSummary || '',
            sameAsCurrent: !/与历史版本\s+\S+\s+内容完全相同/.test(rel.error || ''),
            currentTag: (rel.error || '').match(/与线上正在运行的版本\s+(\S+?)\s+内容完全相同/)?.[1] || '',
            duplicateTag: (rel.error || '').match(/与历史版本\s+(\S+?)\s+内容完全相同/)?.[1] || '',
            currentImage: '',
          };
      showDuplicateDeployModal(info, rel.id);
    } else if (rel.status === 'success') {
      updateStatus('success', '发布完成');
      if (rel.tag) {
        state.currentVersions[rel.app] = { tag: rel.tag, image: rel.image || '', deployedAt: rel.finishedAt || '', error: '' };
      }
    } else if (rel.status === 'failed' || rel.status === 'canceled') {
      updateStatus('failed', rel.status === 'canceled' ? '已中止' : '发布失败');
    } else if (rel.status === 'rolledback') {
      updateStatus('failed', '已自动回滚');
    }
    // 终态立即收起中止按钮：不能等 SSE end，否则发布完成后仍显示可中止
    if (finished) {
      state.isDeploying = false;
      $('startDeployBtn').disabled = false;
      $('cancelReleaseBtn').classList.add('hidden');
    }
  });

  es.addEventListener('log', (e) => {
    const line = JSON.parse(e.data);
    appendLog(line);
  });

  es.addEventListener('duplicate', (e) => {
    // 暂停原因的结构化信息（与线上相同 / 只是与历史版本相同），供 needs_confirm 弹窗使用
    const info = JSON.parse(e.data);
    state.pendingDuplicate = info;
  });

  es.addEventListener('end', () => {
    es.close();
    state.isDeploying = false;
    $('startDeployBtn').disabled = false;
    $('cancelReleaseBtn').classList.add('hidden');
    $('uploadProgressContainer').classList.add('hidden');
    updatePathPreview();
    // 部署结束后线上版本可能已变化：已连接时顺便刷新供下次填写参考
    const curApp = $('deployAppSelect').value;
    if (state.connEnabled && curApp) refreshCurrentVersion(curApp);
  });

  es.onopen = () => { state.esRetry = 0; };
  es.onerror = () => {
    es.close();
    // 网络抖动导致断开时指数退避重连：后端支持历史回放，不会丢失日志
    if (!state.isDeploying || !state.releaseId || state.releaseId !== releaseId) return;
    const retry = state.esRetry || 0;
    state.esRetry = retry + 1;
    setTimeout(() => listenReleaseEvents(releaseId, true), Math.min(30000, 1000 * 2 ** retry));
  };
}

function renderStages(stages) {
  if (!stages || stages.length === 0) return;
  const html = stages.map((s) => {
    const iconName = s.status === 'success' ? 'check'
      : s.status === 'failed' ? 'x'
      : s.status === 'running' ? 'loader'
      : s.status === 'skipped' ? 'check'
      : 'circle';
    return `<div class="stage-chip ${s.status}">
      ${ic(iconName, 'icon stage-icon')}
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

// ---------------- 配置备份与回滚（按服务） ----------------
// ---------------- 镜像维护：平台不自动删除镜像，仅在此处由用户显式勾选 ----------------
function renderImageAppSelect() {
  const sel = $('imageMaintAppSelect');
  if (!sel) return;
  const prev = sel.value || state.prefs.imageMaintAppId || '';
  sel.innerHTML = state.apps
    .map((a) => `<option value="${escapeHtml(a.id)}">${escapeHtml(a.name)} · ${escapeHtml(a.image || '未配置镜像')}</option>`)
    .join('');
  if (prev && state.apps.some((a) => a.id === prev)) {
    sel.value = prev;
  }
}

async function loadImages() {
  const appId = $('imageMaintAppSelect').value;
  const hint = $('imageMaintHint');
  if (!appId) {
    hint.textContent = '请先选择应用。';
    return;
  }
  if (!state.connEnabled) {
    hint.textContent = '尚未连接服务器：请先点击顶部的「连接服务器」，再刷新镜像列表。';
    return;
  }
  state.prefs.imageMaintAppId = appId;
  savePrefs();
  hint.textContent = '正在读取服务器上的镜像列表...';
  const res = await api('GET', `/api/docker/images?appId=${encodeURIComponent(appId)}`);
  if (!res.ok) {
    hint.textContent = `读取失败: ${res.data.error || res.status}`;
    state.imageMaint = { appId, images: [] };
    renderImages();
    return;
  }
  state.imageMaint = { appId, images: res.data.images || [] };
  const parts = [`共 ${state.imageMaint.images.length} 个镜像（${res.data.image} @ ${res.data.server}）`];
  if (res.data.error) parts.push(`读取出错: ${res.data.error}`);
  if (res.data.warning) parts.push(res.data.warning);
  hint.textContent = parts.join(' · ');
  renderImages();
}

function renderImages() {
  const list = $('imageMaintList');
  if (!list) return;
  const images = state.imageMaint.images || [];
  if (images.length === 0) {
    list.innerHTML = '<p class="config-sub-hint" style="padding:8px 0;">暂无镜像，或尚未刷新。</p>';
    syncDeleteImagesBtn();
    return;
  }
  const rows = images
    .map((img) => {
      const ref = `${state.apps.find((a) => a.id === state.imageMaint.appId)?.image || ''}:${img.tag}`;
      const disabled = img.protected || img.inUse;
      const badge = img.protected
        ? `<span class="tag-badge" style="color:var(--success-text);">受保护 · ${escapeHtml(img.protectedReason || '')}</span>`
        : img.inUse
          ? '<span class="tag-badge" style="color:var(--warning-text);">被容器使用</span>'
          : '<span class="tag-badge" style="color:var(--text-muted);">可删除</span>';
      return `<tr>
        <td><input type="checkbox" data-image-ref="${escapeHtml(ref)}" data-image-tag="${escapeHtml(img.tag)}" ${disabled ? 'disabled' : ''}></td>
        <td><code>${escapeHtml(img.tag)}</code></td>
        <td>${escapeHtml(img.id || '-')}</td>
        <td>${escapeHtml(img.size || '-')}</td>
        <td>${escapeHtml(img.createdAt || '-')}</td>
        <td>${badge}</td>
      </tr>`;
    })
    .join('');
  list.innerHTML = `<div class="table-responsive"><table class="data-table">
    <thead><tr><th style="width:40px;"></th><th>Tag</th><th>镜像 ID</th><th>大小</th><th>创建时间</th><th>状态</th></tr></thead>
    <tbody>${rows}</tbody></table></div>`;
  syncDeleteImagesBtn();
}

function selectedImageRefs() {
  return Array.from(document.querySelectorAll('#imageMaintList input[data-image-ref]:checked')).map((el) => el.dataset.imageRef);
}

function syncDeleteImagesBtn() {
  const btn = $('deleteImagesBtn');
  if (!btn) return;
  const n = selectedImageRefs().length;
  btn.disabled = n === 0 || !state.connEnabled;
  btn.textContent = n > 0 ? `删除勾选镜像 (${n})` : '删除勾选镜像';
}

async function deleteSelectedImages() {
  const refs = selectedImageRefs();
  if (refs.length === 0) return;
  const appId = state.imageMaint.appId;
  if (!appId) return;
  const ok = confirm(`确认删除以下 ${refs.length} 个镜像？删除后无法回滚到这些版本：\n\n${refs.join('\n')}`);
  if (!ok) return;
  const res = await api('POST', '/api/docker/images/delete', { appId, refs });
  if (!res.ok) {
    alert(`✖ 删除失败: ${res.data.error || res.status}`);
    return;
  }
  const removed = res.data.removed || [];
  const skipped = res.data.skipped || [];
  const lines = [];
  if (removed.length) lines.push(`已删除 ${removed.length} 个镜像`);
  if (skipped.length) lines.push(`跳过 ${skipped.length} 个：\n${skipped.join('\n')}`);
  alert(lines.join('\n\n') || '没有删除任何镜像');
  await loadImages();
}

// ---------------- 配置备份 ----------------

// 备份文件名形如 order-service_v1.2.0_20261112-201500.000000000.yaml：
// 一眼能看出是哪个服务、哪个版本、什么时候的配置；恢复只替换该服务。
async function loadAppBackups() {
  const box = $('appBackupsList');
  if (!box) return;
  const res = await api('GET', '/api/config/app-backups');
  if (!res.ok) {
    box.innerHTML = '<div class="td-empty">备份列表加载失败</div>';
    return;
  }
  const list = res.data || [];
  if (list.length === 0) {
    box.innerHTML = '<div class="td-empty">暂无按服务的备份（保存或删除应用配置后自动产生）</div>';
    return;
  }
  // 按服务分组展示
  const groups = new Map();
  list.forEach((b) => {
    const key = b.appId || b.appName || 'unknown';
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(b);
  });
  box.innerHTML = Array.from(groups.values()).map((items) => {
    const head = items[0];
    const rows = items.map((b) => `
      <div class="backup-row">
        <span class="backup-name" title="${escapeHtml(b.name)}">${escapeHtml(b.name)}</span>
        <span class="backup-tag">版本 ${escapeHtml(b.tag || 'no-tag')}</span>
        <span class="backup-time">${fmtTime(b.createdAt)}</span>
        <span class="backup-size">${fmtSize(b.size)}</span>
        <button class="btn btn-secondary btn-sm" onclick="restoreAppBackup('${escapeHtml(b.name)}')">恢复此版本</button>
      </div>`).join('');
    return `<div class="backup-group">
      <div class="backup-group-head">
        <strong>${escapeHtml(head.appName || head.appId)}</strong>
        <code>${escapeHtml(head.image || '-')}</code>
        <span class="mini-badge">${items.length} 份备份</span>
      </div>
      ${rows}
    </div>`;
  }).join('');
}

window.restoreAppBackup = async function (name) {
  if (!confirm(`确定恢复备份 ${name} 吗？只会替换该服务的配置，当前版本会自动再备份一次，可再次恢复回来。`)) return;
  const res = await api('POST', '/api/config/app-backups/restore', { name });
  if (!res.ok) {
    alert(`恢复失败: ${res.data.error || '未知错误'}`);
    return;
  }
  await refreshConfigs();
  alert('该服务配置已恢复');
};

// ---------------- K8s Pod 监控 (需求 3) ----------------
let currentPods = [];

const ALL_NS = '*';

// Pod 列表后台静默刷新：状态自己更新，只有切换服务器/命名空间或手动刷新才需要等待
let podAutoTimer = null;
let podFetching = false;
let namespaceLoadedFor = ''; // 已拉取过命名空间列表的服务器，避免每次刷新都跑一遍 kubectl get ns

function startPodAutoRefresh() {
  stopPodAutoRefresh();
  podAutoTimer = setInterval(() => {
    if (!state.connEnabled || document.hidden || !$('tab-pods').classList.contains('active')) return;
    // 正在查看某个 Pod 的日志时不打断弹窗
    const logModal = $('podLogModal');
    if (logModal && !logModal.classList.contains('hidden')) return;
    loadPods(true);
  }, 8000);
}

function stopPodAutoRefresh() {
  if (podAutoTimer) {
    clearInterval(podAutoTimer);
    podAutoTimer = null;
  }
}

// 拉取命名空间列表并回填下拉，保持当前选中的命名空间（不存在时回退到上次记住的 / 第一个）
async function loadNamespaceOptions(srvId, keep) {
  const sel = $('podNamespaceSelect');
  const res = await api('GET', `/api/k8s/namespaces?serverId=${encodeURIComponent(srvId)}`);
  if (!res.ok || !Array.isArray(res.data)) {
    sel.innerHTML = `<option value="${escapeHtml(ALL_NS)}">全部命名空间</option><option value="default">default</option>`;
    return;
  }
  const list = res.data.slice().sort();
  sel.innerHTML = `<option value="${escapeHtml(ALL_NS)}">全部命名空间</option>` +
    list.map((ns) => `<option value="${escapeHtml(ns)}">${escapeHtml(ns)}</option>`).join('');
  const candidates = [keep, state.prefs.podNamespace];
  const hit = candidates.find((v) => v && list.includes(v) && v !== ALL_NS);
  sel.value = hit || list[0] || 'default';
}

// 进入监控页 / 切换服务器：需要时才重新拉取命名空间，然后刷新 Pod
async function loadNamespacesAndPods(reloadNamespaces = true) {
  const srvId = $('podServerSelect').value;
  if (!srvId) return;

  if (reloadNamespaces || namespaceLoadedFor !== srvId) {
    $('podLoading').classList.remove('hidden');
    await loadNamespaceOptions(srvId, $('podNamespaceSelect').value);
    namespaceLoadedFor = srvId;
  }
  await loadPods();
  $('podLoading').classList.add('hidden');
}

// 手动刷新：按钮转圈给出明确反馈，只刷新 Pod 列表本身（命名空间已在进入页面时加载）
async function refreshPodsManually() {
  const btn = $('refreshPodsBtn');
  const icon = $('refreshPodsIcon');
  if (btn.disabled) return;
  btn.disabled = true;
  if (icon) icon.classList.add('spin');
  $('podLoading').classList.remove('hidden');
  try {
    const srvId = $('podServerSelect').value;
    if (srvId && namespaceLoadedFor !== srvId) {
      await loadNamespaceOptions(srvId, $('podNamespaceSelect').value);
      namespaceLoadedFor = srvId;
    }
    await loadPods();
  } finally {
    btn.disabled = false;
    if (icon) icon.classList.remove('spin');
    $('podLoading').classList.add('hidden');
  }
}

// silent=true 用于后台定时刷新：不清空表格、不显示加载提示，避免页面闪烁
async function loadPods(silent = false) {
  const srvId = $('podServerSelect').value;
  const ns = $('podNamespaceSelect').value || 'default';
  if (!srvId || podFetching) return;
  // 记录当前命名空间，切换服务器自动回退到第一个时也能被记住（「全部命名空间」不入偏好）
  if (ns !== ALL_NS && state.prefs.podNamespace !== ns) {
    state.prefs.podNamespace = ns;
    savePrefs();
  }

  podFetching = true;
  if (!silent) {
    $('podLoading').classList.remove('hidden');
    $('podsTableBody').innerHTML = '<tr><td colspan="10" class="td-empty">正在加载 Pod 状态...</td></tr>';
  }

  try {
    const res = await api('GET', `/api/k8s/pods?serverId=${encodeURIComponent(srvId)}&namespace=${encodeURIComponent(ns)}`);
    if (!res.ok) {
      if (!silent) {
        $('podsTableBody').innerHTML = `<tr><td colspan="10" class="td-empty" style="color:var(--danger)">加载失败: ${escapeHtml(res.data.error || '未知错误')}</td></tr>`;
      }
      return;
    }
    currentPods = res.data || [];
    // 全集群视图按命名空间分组排序，便于一眼看清各环境
    if (ns === ALL_NS) {
      currentPods.sort((a, b) => (a.namespace || '').localeCompare(b.namespace || '') || a.name.localeCompare(b.name));
    }
    // 保留搜索框的过滤结果，后台刷新不会把用户筛掉的 Pod 又显示回来
    filterPodsTable();
  } finally {
    podFetching = false;
    if (!silent) $('podLoading').classList.add('hidden');
  }
}

function renderPodsTable(pods) {
  if (pods.length === 0) {
    $('podsTableBody').innerHTML = '<tr><td colspan="10" class="td-empty">当前命名空间下无 Pod 运行</td></tr>';
    return;
  }

  const rows = pods.map((p) => {
    const isRunning = p.status === 'Running';
    const isPending = p.status.includes('Pending') || p.status.includes('Creating');
    const badgeCls = isRunning ? 'pod-status-running' : isPending ? 'pod-status-pending' : 'pod-status-error';

    return `<tr>
      <td><strong>${escapeHtml(p.name)}</strong></td>
      <td><code>${escapeHtml(p.namespace || '-')}</code></td>
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
          <button class="btn btn-secondary btn-sm" title="滚动重启该 Deployment" onclick="restartPodDeployment('${escapeHtml(p.name)}')">${ic('refresh')} 重启</button>
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

// 刷新日志按钮的加载反馈：禁用并让图标转圈
function setPodLogBusy(busy) {
  const btn = $('refreshLogBtn');
  const icon = $('refreshLogIcon');
  if (btn) btn.disabled = busy;
  if (icon) icon.classList.toggle('spin', busy);
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
  $('podLogMeta').textContent = '显示最新 200 行日志';
  $('podLogContent').innerHTML = '<span class="log-debug">正在获取最新日志...</span>';
  $('podLogModal').classList.remove('hidden');

  $('refreshLogBtn').onclick = () => fetchPodLogContent(srvId, ns, currentViewingPod, currentViewingContainer);

  $('downloadPodLogBtn').onclick = downloadPodLog;

  $('autoFollowPodLog').onchange = (e) => {
    stopPodLogFollow();
    if (e.target.checked) {
      podLogTimer = setInterval(() => {
        fetchPodLogContent(srvId, ns, currentViewingPod, currentViewingContainer, true);
      }, 2000);
    }
  };

  await fetchPodLogContent(srvId, ns, currentViewingPod, currentViewingContainer);
};

async function fetchPodLogContent(srvId, ns, podName, containerName, isAuto) {
  if (!isAuto) setPodLogBusy(true);
  try {
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
    if (!isAuto) {
      const now = new Date();
      const pad = (n) => String(n).padStart(2, '0');
      $('podLogMeta').textContent = `显示最新 200 行日志 · 最近刷新 ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
    }
    if ($('autoFollowPodLog').checked) {
      const el = $('podLogContent');
      el.scrollTop = el.scrollHeight;
    }
  } finally {
    if (!isAuto) setPodLogBusy(false);
  }
}

// 下载当前 Pod 日志：文件名带上 Pod 名（去掉随机后缀，保留服务名）、容器名与时间
function downloadPodLog() {
  const raw = window._rawPodLog || '';
  if (!raw) {
    alert('当前没有可下载的日志内容');
    return;
  }
  const parts = String(currentViewingPod || 'pod').split('-');
  const svcName = parts.length >= 3 ? parts.slice(0, parts.length - 2).join('-') : parts.slice(0, 1).join('-');
  const safe = (s) => String(s || '').replace(/[^\w.-]/g, '_');
  const d = new Date();
  const p = (n) => String(n).padStart(2, '0');
  const ts = `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
  const fileName = `${safe(svcName)}_${safe(currentViewingPod)}${currentViewingContainer ? '_' + safe(currentViewingContainer) : ''}_${ts}.log`;
  const blob = new Blob([raw], { type: 'text/plain;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

$('closePodLogBtn').addEventListener('click', () => {
  stopPodLogFollow();
  $('autoFollowPodLog').checked = false;
  setPodLogBusy(false);
  $('podLogModal').classList.add('hidden');
});

window.restartPodDeployment = async function (podName) {
  const srvId = $('podServerSelect').value;
  const ns = $('podNamespaceSelect').value;

  // 推断 Deployment 名称：优先匹配应用配置，必须带 "-" 边界，
  // 否则 deployment "order" 会误匹配 pod "order-service-xxx"，重启到别的服务
  let deployName = '';
  const matchedApp = state.apps.find((a) => a.deployment && podName.startsWith(a.deployment + '-'));
  if (matchedApp) {
    deployName = matchedApp.deployment;
  } else {
    const parts = podName.split('-');
    // Deployment pod 名形如 name-replicaSetHash-suffix；StatefulSet 为 name-序号
    if (parts.length >= 3) {
      deployName = parts.slice(0, parts.length - 2).join('-');
    } else if (parts.length === 2) {
      deployName = parts[0];
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
// 配置页只展示"当前选中的服务器"：下拉选择服务器，其余服务器暂不显示，页面不再堆满连接信息

function currentConfigServer() {
  const sel = $('configServerSelect');
  if (!sel) return null;
  const id = sel.value || state.prefs.configServerId;
  return state.servers.find((s) => s.id === id) || state.servers[0] || null;
}

function renderServerPicker() {
  const sel = $('configServerSelect');
  if (!sel) return;
  if (state.servers.length === 0) {
    sel.innerHTML = '<option value="">暂无服务器</option>';
    sel.disabled = true;
    ['configConnectBtn', 'configEditServerBtn', 'configDeleteServerBtn', 'configDisconnectBtn'].forEach((id) => {
      const el = $(id);
      if (el) el.disabled = true;
    });
    $('serverDetailPanel').innerHTML = '<div class="config-hint">尚未配置服务器，请点击右上角“+ 添加服务器”。</div>';
    const disc = $('configDisconnectBtn');
    if (disc) disc.classList.add('hidden');
    return;
  }
  sel.disabled = false;
  ['configConnectBtn', 'configEditServerBtn', 'configDeleteServerBtn'].forEach((id) => {
    const el = $(id);
    if (el) el.disabled = false;
  });

  const prev = sel.value || state.prefs.configServerId;
  sel.innerHTML = state.servers.map((s) => `<option value="${escapeHtml(s.id)}">${escapeHtml(s.name)} (${escapeHtml(s.host)})</option>`).join('');
  sel.value = state.servers.some((s) => s.id === prev) ? prev : state.servers[0].id;
  state.prefs.configServerId = sel.value;
  savePrefs();
  renderServerDetail();
}

function renderServerDetail() {
  const panel = $('serverDetailPanel');
  if (!panel) return;
  const s = currentConfigServer();
  if (!s) {
    panel.innerHTML = '';
    return;
  }
  const isConnected = state.activeServerIds.includes(s.id);
  const statusBadge = isConnected
    ? '<span class="pod-status-badge pod-status-running">已建立长连接</span>'
    : '<span class="pod-status-badge pod-status-offline">未连接</span>';
  panel.innerHTML = `
    <div class="server-detail-head">
      <span class="server-detail-name">${escapeHtml(s.name)}</span>
      ${statusBadge}
    </div>
    <div class="server-detail-grid">
      <div class="server-detail-item"><span class="sd-label">主机地址</span><code>${escapeHtml(s.host)}</code></div>
      <div class="server-detail-item"><span class="sd-label">SSH 端口</span><code>${s.port || 22}</code></div>
      <div class="server-detail-item"><span class="sd-label">登录用户</span><code>${escapeHtml(s.user)}</code></div>
      <div class="server-detail-item"><span class="sd-label">认证方式</span><code>${s.authType === 'key' ? '私钥认证' : (s.authType === 'agent' ? 'SSH Agent' : '密码认证')}</code></div>
    </div>
    <div class="server-detail-tip">首次连接或发布时自动建立 SSH 长连接并保持心跳；已连接状态下可手动断开释放句柄。</div>
  `;
  const discBtn = $('configDisconnectBtn');
  if (discBtn) {
    discBtn.classList.toggle('hidden', !isConnected);
    discBtn.disabled = !isConnected;
  }
  const connBtn = $('configConnectBtn');
  if (connBtn) connBtn.innerHTML = isConnected ? `${ic('refresh')} 重新检测` : `${ic('bolt')} 连接`;
}

async function connectCurrentServer() {
  const appId = $('deployAppSelect').value;
  const app = state.apps.find((a) => a.id === appId);
  const srv = app ? state.servers.find((s) => s.id === app.serverId) : null;
  if (!app) {
    return alert('请先选择目标应用');
  }
  if (!srv) {
    // 不能兜底连第一台：应用绑定的服务器已被删除或 ID 不匹配时，会静默连错机器
    return alert(`应用 ${app.name} 绑定的服务器不存在，请先在“服务器与目录配置”中检查并重新绑定`);
  }
  const btn = $('connectSrvBtn');
  const origText = btn.textContent;
  btn.textContent = '连接中...';
  btn.disabled = true;
  const res = await api('POST', '/api/servers/test', srv);
  btn.textContent = origText;
  btn.disabled = false;
  if (res.ok) {
    state.connEnabled = true;
    await updateConnStatus();
    // 连接成功前读不到线上版本，这里立即补一次，否则要切换应用才会刷新
    state.currentVersions = {};
    await refreshCurrentVersion(appId);
    maybePrefillTag(appId);
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
  const res = await api('POST', '/api/conn-mode', { enabled: false });
  if (res.ok) {
    state.connEnabled = false;
    await updateConnStatus();
  }
}

async function updateConnStatus() {
  const [modeRes, res] = await Promise.all([
    api('GET', '/api/conn-mode'),
    api('GET', '/api/servers/connections'),
  ]);
  // 服务端开关是唯一权威来源：页面刷新后据此恢复，不会因此自动连接
  if (modeRes.ok) state.connEnabled = !!modeRes.data.enabled;
  syncDeleteImagesBtn();
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
    } else if (state.connEnabled) {
      if (dot) dot.classList.remove('connected');
      txt.textContent = 'SSH 已开启 · 当前无长连接';
      txt.className = 'conn-text';
      if (connBtn) connBtn.classList.remove('hidden');
      if (discBtn) discBtn.classList.remove('hidden');
    } else {
      if (dot) dot.classList.remove('connected');
      txt.textContent = 'SSH 未连接';
      txt.className = 'conn-text';
      if (connBtn) connBtn.classList.remove('hidden');
      if (discBtn) discBtn.classList.add('hidden');
    }
    renderServerDetail();
  }
}

window.testOneServer = async function (srvId) {
  const srv = state.servers.find((s) => s.id === srvId);
  if (!srv) return;
  const res = await api('POST', '/api/servers/test', srv);
  if (res.ok) {
    state.connEnabled = true;
    await updateConnStatus();
    // 连接建立后补拉当前应用的线上版本
    state.currentVersions = {};
    await refreshCurrentVersion($('deployAppSelect').value);
    maybePrefillTag($('deployAppSelect').value);
    alert(`✔ ${res.data.message || '连接测试成功！'}`);
  } else {
    alert(`✖ 连接失败: ${res.data.error || '未知错误'}`);
  }
};

// syncPwdToggle 已保存的密码只显示 ******，此时禁用显示/隐藏按钮
function syncPwdToggle() {
  const input = $('editServerPassword');
  const btn = $('toggleServerPwdBtn');
  if (!input || !btn) return;
  const masked = input.value === '******';
  if (masked) {
    input.type = 'password';
    btn.textContent = '显示';
  }
  btn.disabled = masked;
  btn.title = masked ? '已保存的密码不可查看' : '显示/隐藏输入内容';
}

function bindServerPasswordToggle() {
  const input = $('editServerPassword');
  const btn = $('toggleServerPwdBtn');
  if (!input || !btn) return;
  // 用户清空并输入新密码后，按钮恢复可用
  input.addEventListener('input', syncPwdToggle);
  btn.addEventListener('click', () => {
    if (input.value === '******') return;
    const show = input.type === 'password';
    input.type = show ? 'text' : 'password';
    btn.textContent = show ? '隐藏' : '显示';
  });
}

function openServerModal(srv) {
  $('editServerId').value = srv ? srv.id : '';
  $('editServerName').value = srv ? srv.name : '';
  $('editServerHost').value = srv ? srv.host : '';
  $('editServerPort').value = srv ? srv.port : '22';
  $('editServerUser').value = srv ? srv.user : 'root';
  const authType = srv ? (srv.authType || 'password') : 'password';
  $('editServerAuthType').value = authType;
  $('editServerPassword').value = srv ? (srv.password || '') : '';
  // 后端不再回传真实密码，仅返回 ****** 占位符：编辑时提示保留原密码
  $('editServerPassword').placeholder = srv && srv.password === '******' ? '已保存（留空或保持 ****** 表示不修改）' : '输入密码 (仅保存在本地配置)';
  $('editServerKeyPath').value = srv ? srv.keyPath : '';
  syncPwdToggle();

  // 必须传明确的布尔值：classList.toggle(token, undefined) 等价于单参数 toggle，
  // 会把"新增服务器"时的密码输入框反而隐藏掉（之前的 bug）
  const useKey = authType === 'key';
  $('pwdGroup').classList.toggle('hidden', useKey);
  $('keyGroup').classList.toggle('hidden', !useKey);
  $('serverModal').classList.remove('hidden');
}

window.editServer = function (id) {
  const s = state.servers.find((x) => x.id === id);
  if (s) openServerModal(s);
};

window.deleteServer = async function (id) {
  if (!confirm('确定要删除此服务器配置吗？绑定该服务器的应用将无法发布。')) return;
  const res = await api('DELETE', `/api/servers/${encodeURIComponent(id)}`);
  if (res.ok) {
    if (state.prefs.configServerId === id) state.prefs.configServerId = '';
    savePrefs();
    await refreshConfigs();
  }
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
    // 测试连接成功同样会打开服务端连接开关，同步本地状态
    state.connEnabled = true;
    await updateConnStatus();
    // 连接建立后补拉当前应用的线上版本
    state.currentVersions = {};
    await refreshCurrentVersion($('deployAppSelect').value);
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
  // 保存后直接选中这台服务器，配置页立刻展示它的详情
  state.prefs.configServerId = srv.id;
  savePrefs();
  await refreshConfigs();
}

// ---------------- 应用配置管理 (独立目录配置) ----------------
function renderAppCards() {
  // 只列出当前所选服务器下的服务（未绑定服务器的旧配置始终展示，避免"消失"）
  const srv = currentConfigServer();
  const serverApps = srv ? state.apps.filter((a) => !a.serverId || a.serverId === srv.id) : state.apps;
  if ($('appCountBadge')) {
    $('appCountBadge').textContent = `${serverApps.length} 个服务`;
  }
  if ($('appListHint')) {
    $('appListHint').textContent = srv
      ? `当前服务器「${srv.name}」下的服务；其他服务器的配置暂不显示，可在上方切换服务器查看。`
      : '多个服务共用同一个 K8s YAML 目录（按各自独立的 YAML 文件名区分）。';
  }
  if (serverApps.length === 0) {
    $('appCardsList').innerHTML = `<div class="config-hint">当前服务器${srv ? '「' + escapeHtml(srv.name) + '」' : ''}下暂无应用，请点击右上角“+ 添加应用”。</div>`;
    return;
  }
  const kw = ($('appConfigSearchInput') ? $('appConfigSearchInput').value : '').trim().toLowerCase();
  const filtered = kw
    ? serverApps.filter((a) =>
        (a.name || '').toLowerCase().includes(kw) ||
        (a.image || '').toLowerCase().includes(kw) ||
        (a.k8sYamlFile || '').toLowerCase().includes(kw) ||
        (a.namespace || '').toLowerCase().includes(kw)
      )
    : serverApps;

  if (filtered.length === 0) {
    $('appCardsList').innerHTML = '<div class="config-hint">未找到匹配搜索关键词的服务配置。</div>';
    return;
  }

  $('appCardsList').innerHTML = filtered.map((a) => `
    <div class="item-card">
      <div class="item-card-header">
        <span class="item-card-title">${escapeHtml(a.name)} <span class="app-type-tag">${escapeHtml(a.type)}</span></span>
        <div style="display:flex;gap:6px;">
          <button class="btn btn-secondary btn-sm" onclick="editApp('${escapeHtml(a.id)}')">编辑</button>
          <button class="btn btn-danger btn-sm" onclick="deleteApp('${escapeHtml(a.id)}')">删除</button>
        </div>
      </div>
      ${a.localBuildCmd || a.localArtifactPath ? `<div class="item-card-desc"><strong>本地编译/默认产物:</strong> <code>${escapeHtml(a.localBuildCmd || '仅指定默认产物')}</code> (产物: <code>${escapeHtml(a.localArtifactPath || '-')}</code>)</div>` : ''}
      <div class="item-card-desc"><strong>独立产物目录:</strong> <code>${escapeHtml(a.artifactDir || '-')}</code></div>
      <div class="item-card-desc"><strong>独立构建目录:</strong> <code>${escapeHtml(a.buildDir || '-')}</code></div>
      ${a.deployMode === 'docker'
        ? `<div class="item-card-desc"><strong>部署形态: Docker 容器</strong> · 容器: <code>${escapeHtml(a.dockerContainer || a.deployment || a.name)}</code>${a.dockerPort ? ' · 端口: <code>' + escapeHtml(a.dockerPort) + '</code>' : ''}</div>`
        : `<div class="item-card-desc"><strong>共享 K8s 目录 + 独立 YAML:</strong> <code>${escapeHtml(a.k8sYamlDir || '-')}</code> / <code style="color:var(--primary);font-weight:600;">${escapeHtml(a.k8sYamlFile || (a.name + '.yaml'))}</code></div>`}
      <div class="item-card-desc"><strong>独立镜像与动态 Tag:</strong> <code>${escapeHtml(a.image || '-')}:&lt;Tag&gt;</code></div>
      ${a.deployMode === 'docker' ? '' : `<div class="item-card-desc"><strong>K8s 目标工作负载:</strong> ns: <code>${escapeHtml(a.namespace)}</code>, deployment: <code>${escapeHtml(a.deployment)}</code></div>`}
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
  // 没有可参考的应用时不编造镜像仓库，由用户自己填写
  return '';
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
  $('editAppDeployMode').value = app ? (app.deployMode || 'k8s') : 'k8s';
  $('editAppDeployMode').dataset.orig = app ? (app.deployMode || 'k8s') : 'k8s';
  $('editAppDockerContainer').value = app ? (app.dockerContainer || '') : '';
  $('editAppDockerContainer').dataset.manual = app ? '1' : '0';
  $('editAppDockerPort').value = app ? (app.dockerPort || '') : '';
  $('editAppDockerVolumes').value = app ? (app.dockerVolumes || '') : '';
  $('editAppDockerEnv').value = app ? (app.dockerEnv || '') : '';
  $('editAppDockerRunOpts').value = app ? (app.dockerRunOpts || '') : '';
  $('editAppDockerHealthPath').value = app ? (app.dockerHealthPath || '') : '';
  toggleDockerConfigBox();

  // 渲染绑定服务器选项：新建应用默认绑定配置页当前选中的服务器
  const defaultSrv = currentConfigServer();
  $('editAppServerId').innerHTML = state.servers.map((s) => `
    <option value="${escapeHtml(s.id)}" ${app && app.serverId === s.id ? 'selected' : ''}>${escapeHtml(s.name)} (${escapeHtml(s.host)})</option>
  `).join('');
  if (!app && defaultSrv) {
    $('editAppServerId').value = defaultSrv.id;
  }

  $('appModal').classList.remove('hidden');
}

// 未手工改过容器名时，Docker 容器名跟随应用名/Deployment（与后端 normalize 规则一致）
function syncDockerContainerFromName() {
  const dc = $('editAppDockerContainer');
  if (!dc || dc.dataset.manual === '1') return;
  const name = ($('editAppName').value || '').trim();
  const dep = ($('editAppDeployment').value || '').trim() || name;
  dc.value = dep;
}

// 部署形态切换：只有 Docker 形态才显示容器配置，并同步部署命令模板与默认容器名
function toggleDockerConfigBox() {
  const mode = $('editAppDeployMode').value;
  const box = $('dockerConfigBox');
  if (box) box.classList.toggle('hidden', mode !== 'docker');

  const cmd = $('editAppDeployCmd');
  const placeholder = mode === 'docker'
    ? 'docker run -d --name {{.Container}} {{.Image}}:{{.Tag}}'
    : 'kubectl apply -f {{.K8sYamlPath}}';
  if (cmd) {
    cmd.placeholder = placeholder;
    // 切换了形态且命令还是另一形态的模板：替换成本形态的模板，避免"选了 Docker 仍去 kubectl apply"
    const otherMode = mode === 'docker' ? cmd.value.trim().startsWith('kubectl ') : cmd.value.trim().startsWith('docker ');
    if (otherMode) cmd.value = placeholder;
  }

  // 容器名默认跟随 Deployment / 应用名，与后端 normalize 的规则一致
  const name = ($('editAppName').value || '').trim();
  const dep = ($('editAppDeployment').value || '').trim() || name;
  const dc = $('editAppDockerContainer');
  if (dc && mode === 'docker' && !dc.value.trim()) dc.value = dep;

  // 模板按钮只在对应形态下可用
  const k8sOnly = mode !== 'docker';
  ['tplYamlApplyBtn', 'tplSetImageBtn'].forEach((id) => {
    const btn = $(id);
    if (btn) btn.disabled = !k8sOnly;
  });
}

// 未保存就关闭应用配置弹窗时，记下构建目录/构建命令草稿：少了这个，"编辑后不保存直接发布"会按旧配置构建失败
function stashAppDraft() {
  const id = $('editAppId').value;
  if (!id) return;
  state.appDrafts = state.appDrafts || {};
  state.appDrafts[id] = {
    buildDir: $('editAppBuildDir').value.trim(),
    buildCmd: $('editAppBuildCmd').value.trim(),
  };
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
    deployMode: $('editAppDeployMode').value || 'k8s',
    dockerContainer: $('editAppDockerContainer').value.trim(),
    dockerPort: $('editAppDockerPort').value.trim(),
    dockerVolumes: $('editAppDockerVolumes').value.trim(),
    dockerEnv: $('editAppDockerEnv').value.trim(),
    dockerRunOpts: $('editAppDockerRunOpts').value.trim(),
    dockerHealthPath: $('editAppDockerHealthPath').value.trim(),
    rolloutTimeout: 300,
    autoRollback: true,
  };

  const res = await api('POST', '/api/apps', app);
  if (!res.ok) {
    alert('保存应用失败: ' + res.data.error);
    return;
  }

  $('appModal').classList.add('hidden');
  if (state.appDrafts) delete state.appDrafts[app.id];
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
  } else if (statusFilter === 'dup') {
    filtered = filtered.filter((r) => r.duplicateOf);
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
    const statusText = isSuccess ? '发布成功'
      : r.status === 'running' ? '进行中'
      : r.status === 'needs_confirm' ? '待确认'
      : r.status === 'canceled' ? '已中止'
      : r.status === 'rolledback' ? '已自动回滚'
      : '发布失败';
    const statusCls = isSuccess ? 'status-success'
      : r.status === 'running' ? 'status-running'
      : r.status === 'needs_confirm' ? 'status-notice'
      : r.status === 'canceled' || r.status === 'rolledback' ? 'status-muted'
      : 'status-failed';

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
          <div style="font-weight:600;color:var(--text-main)">${fmtTime(r.createdAt)}</div>
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
            <button class="copy-tag-btn" title="复制版本号" onclick="copyText('${escapeHtml(r.tag)}')">${ic('copy', 'icon icon-inline')}</button>
          </div>
          ${r.force ? '<div class="force-tag-warn">强制覆盖版本</div>' : ''}
          ${r.duplicateOf ? '<div class="duplicate-tag-note" title="镜像内容与已发布版本完全相同">镜像内容重复</div>' : ''}
        </td>
        <td>
          <span class="status-pill ${statusCls}">
            ${statusText}
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
        <td style="color:var(--text-secondary);font-size:12px;max-width:180px;word-break:break-all">
          ${escapeHtml(r.remark || '-')}
        </td>
        <td>
          <div class="history-row-actions">
            <button class="btn btn-secondary btn-sm" onclick="openReleaseLogModal('${escapeHtml(r.id)}', '${escapeHtml(r.app)}', '${escapeHtml(r.tag)}', '${escapeHtml(r.createdAt || '')}')">
              ${ic('file')} 查看日志
            </button>
            <button class="btn btn-secondary btn-sm" title="填入发布面板" onclick="reuseReleaseTag('${escapeHtml(r.app)}', '${escapeHtml(r.tag)}')">
              ${ic('rocket')} 选用
            </button>
            ${isSuccess ? `
              <button class="btn btn-warn btn-sm" title="回滚线上至此历史版本" onclick="triggerRollback('${escapeHtml(r.id)}')">
                ${ic('undo')} 回滚
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

// buildLogFileName 生成下载文件名：服务名 + Tag + 发布时间，避免下载后分不清是哪个服务
function buildLogFileName(app, tag, id, createdAt) {
  const safe = (s) => String(s || '').replace(/[^\w.-]/g, '_');
  let ts = '';
  const d = createdAt ? new Date(createdAt) : null;
  if (d && !isNaN(d.getTime())) {
    const p = (n) => String(n).padStart(2, '0');
    ts = `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
  } else {
    ts = String(id || '').split('-')[0];
  }
  return `${safe(app)}_${safe(tag)}_${ts}.log`;
}

window.openReleaseLogModal = async function (id, app, tag, createdAt) {
  $('releaseLogModalTitle').textContent = `发布日志: ${app} [${tag}]`;
  $('releaseLogModalSub').textContent = `任务编号: ${id}`;
  $('releaseLogModalBody').innerHTML = '<span class="log-debug">正在获取构建日志...</span>';
  const link = $('downloadReleaseLogLink');
  link.href = `/api/releases/${id}/log`;
  link.download = buildLogFileName(app, tag, id, createdAt);
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
