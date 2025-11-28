const { ipcRenderer } = require('electron');

// 平台列表（只保留7个平台）
const platforms = [
  "京东", "天猫", "抖音", "快手", "微信", "小红书", "拼多多"
];

// 平台店铺配置
let platformStores = {};

// 全局数据库配置
let globalDbConfig = {
  host: 'localhost',
  port: 3306,
  database: 'datas',
  user: 'root',
  password: ''
};

// 显示数据库配置对话框
function showDbConfig() {
  document.getElementById('global-db-host').value = globalDbConfig.host;
  document.getElementById('global-db-port').value = globalDbConfig.port;
  document.getElementById('global-db-name').value = globalDbConfig.database;
  document.getElementById('global-db-user').value = globalDbConfig.user;
  document.getElementById('global-db-password').value = globalDbConfig.password;
  document.getElementById('db-config-dialog').style.display = 'flex';
}



// 获取数据库配置
function getDbConfig() {
  return globalDbConfig;
}

// 初始化平台列表
function initPlatformList() {
  const container = document.getElementById('platform-list');
  container.innerHTML = platforms.map(platform => `
    <div class="platform-config-item">
      <div class="platform-header">
        <input type="checkbox" id="platform-${platform}" value="${platform}" onchange="togglePlatform('${platform}', this.checked); updateGenerateEstimate();">
        <label for="platform-${platform}">${platform}</label>
        <div class="store-count-input">
          <label>店铺数:</label>
          <input type="number" id="count-${platform}" value="4" min="1" max="20" onchange="updateStoreCount('${platform}', this.value); updateGenerateEstimate();" />
        </div>
      </div>
      <div class="store-list" id="stores-${platform}" style="display: none;"></div>
    </div>
  `).join('');
}

// 切换平台选择
function togglePlatform(platform, checked) {
  const storeList = document.getElementById(`stores-${platform}`);
  const countInput = document.getElementById(`count-${platform}`);
  
  if (checked) {
    const count = parseInt(countInput.value) || 4;
    platformStores[platform] = generateStoreNames(platform, count);
    renderStoreList(platform);
    storeList.style.display = 'block';
  } else {
    delete platformStores[platform];
    storeList.style.display = 'none';
  }
}

// 更新店铺数量
function updateStoreCount(platform, count) {
  const checkbox = document.getElementById(`platform-${platform}`);
  if (!checkbox.checked) return;
  
  count = parseInt(count) || 4;
  const currentStores = platformStores[platform] || [];
  
  if (count > currentStores.length) {
    // 增加店铺
    const newStores = generateStoreNames(platform, count - currentStores.length);
    platformStores[platform] = [...currentStores, ...newStores];
  } else if (count < currentStores.length) {
    // 减少店铺
    platformStores[platform] = currentStores.slice(0, count);
  }
  
  renderStoreList(platform);
}

// 生成店铺名称
function generateStoreNames(platform, count) {
  const suffixes = ['旗舰店', '专卖店', '官方店', '直营店', '精品店', '体验店'];
  const prefixes = ['', '官方', '正品', '品牌', '优选'];
  const stores = [];
  
  for (let i = 0; i < count; i++) {
    const prefix = prefixes[Math.floor(Math.random() * prefixes.length)];
    const suffix = suffixes[Math.floor(Math.random() * suffixes.length)];
    const num = i + 1;
    stores.push(`${prefix}${platform}${suffix}${num}号`);
  }
  
  return stores;
}

// 渲染店铺列表
function renderStoreList(platform) {
  const container = document.getElementById(`stores-${platform}`);
  const stores = platformStores[platform] || [];
  
  container.innerHTML = `
    <div class="store-list-header">
      <span>店铺名称</span>
      <button class="btn-small btn-secondary" onclick="regenerateStores('${platform}')">🔄 重新生成</button>
    </div>
    ${stores.map((store, idx) => `
      <div class="store-item">
        <span class="store-number">${idx + 1}</span>
        <input type="text" class="store-name-input" value="${store}" onchange="updateStoreName('${platform}', ${idx}, this.value)" />
      </div>
    `).join('')}
  `;
}

// 重新生成店铺名称
function regenerateStores(platform) {
  const count = platformStores[platform]?.length || 4;
  platformStores[platform] = generateStoreNames(platform, count);
  renderStoreList(platform);
  showToast(`已重新生成${platform}的店铺名称`, 'success');
}

// 更新店铺名称
function updateStoreName(platform, idx, name) {
  if (platformStores[platform]) {
    platformStores[platform][idx] = name;
  }
}

// 显示生成配置对话框
function showGenerateConfig() {
  initPlatformList();
  loadConfigFromFile();
  document.getElementById('generate-dialog').style.display = 'flex';
}

// 显示导入数据库配置对话框
function showLoadConfig() {
  document.getElementById('load-dialog').style.display = 'flex';
}

// 加载配置文件
async function loadConfigFromFile() {
  try {
    const result = await ipcRenderer.invoke('load-generate-config');
    if (result.success && result.config) {
      const config = result.config;
      
      // 加载平台店铺配置
      if (config.platformStores) {
        platformStores = config.platformStores;
        Object.keys(platformStores).forEach(platform => {
          const checkbox = document.getElementById(`platform-${platform}`);
          if (checkbox) {
            checkbox.checked = true;
            const countInput = document.getElementById(`count-${platform}`);
            countInput.value = platformStores[platform].length;
            togglePlatform(platform, true);
          }
        });
      }
      
      // 加载数据量配置
      if (config.numOrders) {
        document.getElementById('num-orders').value = config.numOrders;
      }
      if (config.timeSpanDays) {
        document.getElementById('time-span-days').value = config.timeSpanDays;
      }
      
      // 加载主营类目
      if (config.mainCategory) {
        const radio = document.getElementById(`cat-${config.mainCategory}`);
        if (radio) radio.checked = true;
      }
      
      updateGenerateEstimate();
    }
  } catch (error) {
    console.error('加载配置失败:', error);
  }
}

// 保存配置到文件
async function saveConfig() {
  const config = {
    platformStores: platformStores,
    numOrders: parseInt(document.getElementById('num-orders').value),
    timeSpanDays: parseInt(document.getElementById('time-span-days').value),
    mainCategory: document.querySelector('input[name="main-category"]:checked').value
  };
  
  try {
    const result = await ipcRenderer.invoke('save-generate-config', config);
    if (result.success) {
      showToast('配置已保存', 'success');
    } else {
      showToast('保存失败', 'error');
    }
  } catch (error) {
    showToast(`保存失败: ${error.message}`, 'error');
  }
}

// 加载配置（按钮）
async function loadConfig() {
  await loadConfigFromFile();
  showToast('配置已加载', 'success');
}

// 关闭对话框
function closeDialog(dialogId) {
  document.getElementById(dialogId).style.display = 'none';
}

// 更新状态
function updateStatus(step, status, icon, color) {
  const statusEl = document.getElementById(`status-${step}`);
  statusEl.textContent = `${icon} ${status}`;
  statusEl.style.color = color;
  
  // 控制按钮显示
  const execBtn = document.getElementById(`exec-btn-${step}`);
  const stopBtn = document.getElementById(`stop-btn-${step}`);
  const estimateEl = document.getElementById(`estimate-${step}`);
  
  if (status === '执行中...') {
    if (execBtn) execBtn.style.display = 'none';
    if (stopBtn) stopBtn.style.display = 'inline-block';
  } else {
    if (execBtn) execBtn.style.display = 'inline-block';
    if (stopBtn) stopBtn.style.display = 'none';
    if (estimateEl) estimateEl.style.display = 'none';
  }
}

// 显示预估时间并开始智能倒计时
function showEstimate(step, seconds) {
  const estimateEl = document.getElementById(`estimate-${step}`);
  if (estimateEl && seconds > 0) {
    estimateEl.style.display = 'inline-block';
    
    // 清除之前的定时器
    if (countdownTimers[step]) {
      clearInterval(countdownTimers[step]);
    }
    
    // 记录开始时间
    stepStartTime[step] = Date.now();
    let initialEstimate = seconds;
    
    // 更新显示
    const updateDisplay = () => {
      const elapsed = Math.floor((Date.now() - stepStartTime[step]) / 1000);
      const progress = stepProgress[step];
      
      let remaining;
      
      // 如果有进度信息，根据进度计算剩余时间
      if (progress.total > 0 && progress.current > 0) {
        const progressRate = progress.current / progress.total;
        const estimatedTotal = elapsed / progressRate;
        remaining = Math.max(0, Math.ceil(estimatedTotal - elapsed));
      } else {
        // 否则使用简单倒计时，但不低于0
        remaining = Math.max(0, initialEstimate - elapsed);
      }
      
      if (remaining === 0) {
        estimateEl.textContent = '⏱ 即将完成...';
      } else {
        const minutes = Math.floor(remaining / 60);
        const secs = remaining % 60;
        const timeText = minutes > 0 ? `${minutes}分${secs}秒` : `${secs}秒`;
        estimateEl.textContent = `⏱ 剩余约 ${timeText}`;
      }
    };
    
    updateDisplay();
    
    // 每秒更新
    countdownTimers[step] = setInterval(updateDisplay, 1000);
  }
}

// 更新步骤进度（从日志中提取进度信息）
function updateStepProgress(step, message) {
  // 尝试从消息中提取进度信息，如 "(5/10) 50%"
  const progressMatch = message.match(/\((\d+)\/(\d+)\)/);
  if (progressMatch) {
    stepProgress[step].current = parseInt(progressMatch[1]);
    stepProgress[step].total = parseInt(progressMatch[2]);
  }
}

// 停止倒计时
function stopEstimate(step) {
  if (countdownTimers[step]) {
    clearInterval(countdownTimers[step]);
    countdownTimers[step] = null;
  }
  stepStartTime[step] = null;
  stepProgress[step] = { current: 0, total: 0 };
  
  const estimateEl = document.getElementById(`estimate-${step}`);
  if (estimateEl) {
    estimateEl.style.display = 'none';
  }
}

// 更新生成配置的预估时间
function updateGenerateEstimate() {
  const numOrders = parseInt(document.getElementById('num-orders').value) || 20000;
  
  // 基于订单数预估时间（每万条订单约5-10秒）
  const estimateSeconds = Math.ceil((numOrders / 10000) * 7) + 5;
  
  const minutes = Math.floor(estimateSeconds / 60);
  const secs = estimateSeconds % 60;
  const timeText = minutes > 0 ? `${minutes}分${secs}秒` : `${secs}秒`;
  
  const estimateTimeEl = document.getElementById('generate-estimate-time');
  if (estimateTimeEl) {
    estimateTimeEl.textContent = timeText;
  }
}

// 停止进程
async function stopProcess(step) {
  const processId = currentProcessIds[step];
  if (!processId) {
    showToast('没有正在运行的进程', 'warning');
    return;
  }
  
  try {
    await ipcRenderer.invoke('stop-process', processId);
    updateStatus(step, '已停止', '⏹', '#f59e0b');
    showToast('进程已停止', 'info');
    currentProcessIds[step] = null;
  } catch (error) {
    showToast(`停止失败: ${error.message}`, 'error');
  }
}

// 显示轻量级通知
function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  
  const icons = {
    success: '✅',
    error: '❌',
    info: 'ℹ️',
    warning: '⚠️'
  };
  
  toast.innerHTML = `
    <span class="toast-icon">${icons[type]}</span>
    <span class="toast-message">${message}</span>
  `;
  
  container.appendChild(toast);
  
  setTimeout(() => toast.classList.add('show'), 10);
  
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 3000);
}

// 开始生成ODS数据（只生成CSV，不导入数据库）
async function startGenerate() {
  if (Object.keys(platformStores).length === 0) {
    showToast('请至少选择一个平台！', 'warning');
    return;
  }

  const numOrders = parseInt(document.getElementById('num-orders').value);
  const timeSpanDays = parseInt(document.getElementById('time-span-days').value);
  const mainCategory = document.querySelector('input[name="main-category"]:checked').value;

  // 先保存配置
  await saveConfig();

  closeDialog('generate-dialog');
  updateStatus(0, '执行中...', '🔄', '#ed8936');
  
  // 标记进程正在运行
  currentProcessIds[0] = 'generate-ods';
  
  // 计算预估时间
  const estimateSeconds = Math.ceil((numOrders / 10000) * 7) + 5;
  showEstimate(0, estimateSeconds);
  
  // 显示步骤进度
  showStepProgress(0);
  appendStepLog(0, '开始生成ODS层数据...');

  try {
    const result = await ipcRenderer.invoke('generate-ods', {
      platformStores,
      numOrders,
      timeSpanDays,
      mainCategory
    });

    if (result.success) {
      updateStatus(0, '已完成', '✅', '#48bb78');
      appendStepLog(0, '\n[完成] CSV数据生成完成！');
      showToast('CSV数据生成完成！可以点击"导入数据库"按钮导入', 'success');
    }
  } catch (error) {
    updateStatus(0, '失败', '❌', '#f56565');
    appendStepLog(0, `\n[错误] ${error.message}`);
    showToast(`执行失败: ${error.message}`, 'error');
  } finally {
    hideStepProgress(0);
    stopEstimate(0);
    currentProcessIds[0] = null;
  }
}

// 开始导入数据库
async function startLoad() {
  const mode = document.querySelector('input[name="load-mode"]:checked').value;
  
  closeDialog('load-dialog');
  updateStatus(0, '执行中...', '🔄', '#ed8936');
  
  currentProcessIds[0] = 'load-ods';
  showEstimate(0, 20);
  
  showStepProgress(0);
  appendStepLog(0, '开始导入ODS数据到数据库...');

  try {
    const result = await ipcRenderer.invoke('load-to-database', {
      layer: 'ods',
      mode: mode,
      dbConfig: getDbConfig()
    });
    
    if (result.success) {
      updateStatusWithSql(0, '已完成', '✅', '#48bb78');
      appendStepLog(0, '\n[完成] 数据已导入到数据库！');
      showToast('数据已导入到数据库！', 'success');
    }
  } catch (error) {
    updateStatus(0, '失败', '❌', '#f56565');
    appendStepLog(0, `\n[错误] ${error.message}`);
    showToast(`导入失败: ${error.message}`, 'error');
  } finally {
    hideStepProgress(0);
    stopEstimate(0);
    currentProcessIds[0] = null;
  }
}

// 生成DWD层（直接在数据库中转换）
async function generateDwd() {
  updateStatus(1, '执行中...', '🔄', '#ed8936');
  
  // 标记进程正在运行
  currentProcessIds[1] = 'generate-dwd';
  
  // DWD层预估时间（基于ODS数据量，假设需要15-30秒）
  showEstimate(1, 20);
  
  // 显示步骤进度
  showStepProgress(1);
  appendStepLog(1, '开始转换DWD层数据...');

  try {
    const result = await ipcRenderer.invoke('generate-dwd', {
      mode: 'full',
      dbConfig: getDbConfig()
    });
    
    if (result.success) {
      updateStatusWithSql(1, '已完成', '✅', '#48bb78');
      appendStepLog(1, '\n[完成] DWD层数据转换完成！');
      showToast('DWD层数据转换完成！', 'success');
    }
  } catch (error) {
    updateStatus(1, '失败', '❌', '#f56565');
    appendStepLog(1, `\n[错误] ${error.message}`);
    showToast(`转换失败: ${error.message}`, 'error');
  } finally {
    hideStepProgress(1);
    stopEstimate(1);
    currentProcessIds[1] = null;
  }
}

// 生成DWS层（直接在数据库中转换）
async function generateDws() {
  updateStatus(2, '执行中...', '🔄', '#ed8936');
  
  // 标记进程正在运行
  currentProcessIds[2] = 'generate-dws';
  
  // DWS层预估时间（基于DWD数据量，假设需要10-20秒）
  showEstimate(2, 15);
  
  // 显示步骤进度
  showStepProgress(2);
  appendStepLog(2, '开始转换DWS层数据...');

  try {
    const result = await ipcRenderer.invoke('generate-dws', {
      mode: 'full',
      dbConfig: getDbConfig()
    });
    
    if (result.success) {
      updateStatusWithSql(2, '已完成', '✅', '#48bb78');
      appendStepLog(2, '\n[完成] DWS层数据转换完成！');
      showToast('DWS层数据转换完成！', 'success');
    }
  } catch (error) {
    updateStatus(2, '失败', '❌', '#f56565');
    appendStepLog(2, `\n[错误] ${error.message}`);
    showToast(`转换失败: ${error.message}`, 'error');
  } finally {
    hideStepProgress(2);
    stopEstimate(2);
    currentProcessIds[2] = null;
  }
}

// 预览数据
async function previewData(layer, step) {
  const dialog = document.getElementById('preview-dialog');
  const title = document.getElementById('preview-title');
  const content = document.getElementById('preview-content');
  
  title.textContent = `${layer.toUpperCase()}层数据预览`;
  content.innerHTML = '<div class="loading">⏳ 加载中...</div>';
  dialog.style.display = 'flex';

  try {
    const result = await ipcRenderer.invoke('preview-data', layer);
    if (result.success) {
      let html = '<div class="preview-files">';
      result.data.forEach(item => {
        const lines = item.content.split('\n');
        const headers = lines[0] ? lines[0].split(',') : [];
        const rows = lines.slice(1, 11);
        
        html += `
          <div class="preview-file">
            <div class="preview-file-header">
              <span class="preview-file-name">📄 ${item.file}</span>
              <span class="preview-file-info">${lines.length - 1} 行数据</span>
            </div>
            <div class="preview-table-wrapper">
              <table class="preview-table">
                <thead>
                  <tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr>
                </thead>
                <tbody>
                  ${rows.map(row => {
                    const cells = row.split(',');
                    return `<tr>${cells.map(c => `<td>${c}</td>`).join('')}</tr>`;
                  }).join('')}
                </tbody>
              </table>
            </div>
          </div>
        `;
      });
      html += '</div>';
      content.innerHTML = html;
    } else {
      content.innerHTML = `<div class="preview-error">❌ ${result.message}</div>`;
    }
  } catch (error) {
    content.innerHTML = `<div class="preview-error">❌ 预览失败: ${error.message}</div>`;
  }
}

// 当前运行的步骤
let currentRunningStep = null;

// 当前运行的进程ID
let currentProcessIds = {
  0: null,  // ODS
  1: null,  // DWD
  2: null   // DWS
};

// 倒计时定时器
let countdownTimers = {
  0: null,
  1: null,
  2: null
};

// 步骤开始时间
let stepStartTime = {
  0: null,
  1: null,
  2: null
};

// 步骤进度信息
let stepProgress = {
  0: { current: 0, total: 0 },
  1: { current: 0, total: 0 },
  2: { current: 0, total: 0 }
};

// 监听日志消息
ipcRenderer.on('log-message', (event, message) => {
  console.log(message);
  
  // 如果有正在运行的步骤，显示日志
  if (currentRunningStep !== null) {
    appendStepLog(currentRunningStep, message);
  }
  
  // 如果清空数据对话框打开，也显示日志
  const progressDialog = document.getElementById('progress-dialog');
  if (progressDialog && progressDialog.style.display === 'flex') {
    appendProgressLog(message);
  }
});

// 添加步骤日志（只显示最新一行）
function appendStepLog(step, message) {
  const logContainer = document.getElementById(`progress-log-${step}`);
  if (!logContainer) return;
  
  // 更新进度信息（用于智能时间预估）
  updateStepProgress(step, message);
  
  // 过滤：只显示重要信息
  const isImportant = 
    message.includes('[错误]') || 
    message.includes('✗') || 
    message.includes('失败') ||
    message.includes('[完成]') || 
    message.includes('✓') ||
    message.includes('完成') ||
    message.includes('[进度]') ||
    message.includes('开始') ||
    message.includes('生成') ||
    message.includes('保存') ||
    message.includes('加载') ||
    message.includes('转换') ||
    message.includes('创建') ||
    message.includes('正在') ||
    message.includes('%');
  
  if (!isImportant) return;
  
  // 清空之前的内容，只显示最新一行
  logContainer.innerHTML = '';
  
  const logLine = document.createElement('div');
  logLine.className = 'log-line';
  
  // 根据消息类型设置样式
  if (message.includes('[错误]') || message.includes('✗') || message.includes('失败')) {
    logLine.classList.add('log-error');
  } else if (message.includes('[完成]') || message.includes('✓') || message.includes('完成')) {
    logLine.classList.add('log-success');
  } else if (message.includes('[进度]') || message.includes('%')) {
    logLine.classList.add('log-progress');
  } else if (message.includes('[配置]') || message.includes('[信息]')) {
    logLine.classList.add('log-info');
  }
  
  logLine.textContent = message;
  logContainer.appendChild(logLine);
}

// 显示步骤进度
function showStepProgress(step) {
  const progressEl = document.getElementById(`step-progress-${step}`);
  const logContainer = document.getElementById(`progress-log-${step}`);
  if (progressEl && logContainer) {
    logContainer.innerHTML = '';
    progressEl.style.display = 'block';
    currentRunningStep = step;
  }
}

// 隐藏步骤进度
function hideStepProgress(step) {
  currentRunningStep = null;
}

// 添加进度日志（用于清空数据对话框，限制显示行数）
function appendProgressLog(message) {
  const logContainer = document.getElementById('progress-log');
  if (!logContainer) return;
  
  const logLine = document.createElement('div');
  logLine.className = 'log-line';
  
  // 根据消息类型设置样式
  if (message.includes('[错误]') || message.includes('✗') || message.includes('失败')) {
    logLine.classList.add('log-error');
  } else if (message.includes('[完成]') || message.includes('✓') || message.includes('完成')) {
    logLine.classList.add('log-success');
  } else if (message.includes('[进度]')) {
    logLine.classList.add('log-progress');
  } else if (message.includes('[配置]') || message.includes('[信息]')) {
    logLine.classList.add('log-info');
  }
  
  logLine.textContent = message;
  logContainer.appendChild(logLine);
  
  // 限制最多显示100行
  const maxLines = 100;
  const lines = logContainer.children;
  if (lines.length > maxLines) {
    const removeCount = lines.length - maxLines;
    for (let i = 0; i < removeCount; i++) {
      logContainer.removeChild(lines[0]);
    }
  }
  
  // 自动滚动到底部
  const progressLogContainer = logContainer.parentElement;
  if (progressLogContainer) {
    progressLogContainer.scrollTop = progressLogContainer.scrollHeight;
  }
}

// 显示进度对话框（用于清空数据）
function showProgressDialog(title) {
  document.getElementById('progress-title').textContent = title;
  document.getElementById('progress-log').innerHTML = '';
  document.getElementById('progress-dialog').style.display = 'flex';
}

// 关闭进度对话框
function closeProgressDialog() {
  document.getElementById('progress-dialog').style.display = 'none';
}

// 窗口控制
document.getElementById('minimize-btn').addEventListener('click', () => {
  ipcRenderer.send('window-minimize');
});

document.getElementById('maximize-btn').addEventListener('click', () => {
  ipcRenderer.send('window-maximize');
});

document.getElementById('close-btn').addEventListener('click', () => {
  ipcRenderer.send('window-close');
});

// 更新数据库连接状态
async function updateDbStatus() {
  const statusDot = document.getElementById('db-status-dot');
  const statusText = document.getElementById('db-status-text');
  
  // 设置检测中状态
  statusDot.className = 'db-status-dot checking';
  statusText.textContent = '检测中...';
  
  try {
    const result = await ipcRenderer.invoke('test-db-connection', globalDbConfig);
    
    if (result.connected) {
      statusDot.className = 'db-status-dot connected';
      statusText.textContent = '已连接';
    } else {
      statusDot.className = 'db-status-dot disconnected';
      statusText.textContent = '未连接';
    }
  } catch (error) {
    statusDot.className = 'db-status-dot disconnected';
    statusText.textContent = '连接失败';
  }
}

// 保存数据库配置
async function saveDbConfig() {
  globalDbConfig = {
    host: document.getElementById('global-db-host').value,
    port: parseInt(document.getElementById('global-db-port').value),
    database: document.getElementById('global-db-name').value,
    user: document.getElementById('global-db-user').value,
    password: document.getElementById('global-db-password').value
  };
  
  try {
    const result = await ipcRenderer.invoke('save-db-config', globalDbConfig);
    if (result.success) {
      closeDialog('db-config-dialog');
      showToast('数据库配置已保存', 'success');
      // 保存后立即测试连接
      updateDbStatus();
    } else {
      showToast(`保存失败: ${result.message}`, 'error');
    }
  } catch (error) {
    showToast(`保存失败: ${error.message}`, 'error');
  }
}

// 初始化
document.addEventListener('DOMContentLoaded', async () => {
  // 加载数据库配置
  try {
    const result = await ipcRenderer.invoke('load-db-config');
    if (result.success && result.config) {
      globalDbConfig = result.config;
      // 加载配置后测试连接
      updateDbStatus();
    }
  } catch (error) {
    console.error('加载数据库配置失败:', error);
  }
  
  // 每30秒自动检测一次连接状态
  setInterval(updateDbStatus, 30000);
});


// SQL信息数据
const sqlInfo = {
  0: { // ODS层
    tables: [
      { name: 'ods_stores', desc: '店铺表', sql: 'CREATE TABLE ods_stores (\n  店铺ID VARCHAR(10) PRIMARY KEY,\n  店铺名称 VARCHAR(100),\n  平台 VARCHAR(50),\n  开店日期 DATE\n);' },
      { name: 'ods_products', desc: '商品表', sql: 'CREATE TABLE ods_products (\n  商品ID VARCHAR(10) PRIMARY KEY,\n  店铺ID VARCHAR(10),\n  平台 VARCHAR(50),\n  商品名称 VARCHAR(200),\n  一级类目 VARCHAR(50),\n  二级类目 VARCHAR(50),\n  售价 DECIMAL(10,2),\n  成本 DECIMAL(10,2),\n  库存 INT\n);' },
      { name: 'ods_users', desc: '用户表', sql: 'CREATE TABLE ods_users (\n  用户ID VARCHAR(10) PRIMARY KEY,\n  用户名 VARCHAR(100),\n  性别 VARCHAR(10),\n  年龄 INT,\n  城市 VARCHAR(50),\n  注册日期 DATE\n);' },
      { name: 'ods_orders', desc: '订单表', sql: 'CREATE TABLE ods_orders (\n  订单ID VARCHAR(10) PRIMARY KEY,\n  用户ID VARCHAR(10),\n  店铺ID VARCHAR(10),\n  平台 VARCHAR(50),\n  下单时间 DATETIME,\n  订单状态 VARCHAR(20),\n  商品总额 DECIMAL(10,2),\n  运费 DECIMAL(10,2),\n  实付金额 DECIMAL(10,2)\n);' },
      { name: 'ods_order_details', desc: '订单明细表', sql: 'CREATE TABLE ods_order_details (\n  订单明细ID VARCHAR(10) PRIMARY KEY,\n  订单ID VARCHAR(10),\n  商品ID VARCHAR(10),\n  数量 INT,\n  单价 DECIMAL(10,2),\n  金额 DECIMAL(10,2)\n);' },
      { name: 'ods_promotion', desc: '推广表', sql: 'CREATE TABLE ods_promotion (\n  推广ID VARCHAR(10) PRIMARY KEY,\n  日期 DATE,\n  店铺ID VARCHAR(10),\n  平台 VARCHAR(50),\n  商品ID VARCHAR(10),\n  一级类目 VARCHAR(50),\n  二级类目 VARCHAR(50),\n  推广渠道 VARCHAR(50),\n  推广花费 DECIMAL(10,2),\n  曝光量 INT,\n  点击量 INT\n);' },
      { name: 'ods_traffic', desc: '流量表', sql: 'CREATE TABLE ods_traffic (\n  日期 DATE,\n  店铺ID VARCHAR(10),\n  平台 VARCHAR(50),\n  访客数 INT,\n  浏览量 INT,\n  搜索流量 INT,\n  推荐流量 INT,\n  直接访问 INT\n);' },
      { name: 'ods_inventory', desc: '库存表', sql: 'CREATE TABLE ods_inventory (\n  库存记录ID VARCHAR(10) PRIMARY KEY,\n  日期 DATE,\n  商品ID VARCHAR(10),\n  店铺ID VARCHAR(10),\n  变动类型 VARCHAR(20),\n  变动数量 INT,\n  变动后库存 INT\n);' }
    ]
  },
  1: { // DWD层
    tables: [
      { name: 'dwd_order_fact', desc: '订单事实表', sql: 'CREATE TABLE dwd_order_fact AS\nSELECT \n  o.订单ID, o.用户ID, o.店铺ID, o.平台,\n  o.下单时间, o.订单状态, o.实付金额,\n  u.性别, u.年龄, u.年龄段, u.城市,\n  s.店铺名称,\n  DATE(o.下单时间) AS 订单日期,\n  YEAR(o.下单时间) AS 年,\n  MONTH(o.下单时间) AS 月,\n  SUM(od.成本金额) AS 成本总额,\n  SUM(od.毛利) AS 毛利\nFROM ods_orders o\nLEFT JOIN ods_users u ON o.用户ID = u.用户ID\nLEFT JOIN ods_stores s ON o.店铺ID = s.店铺ID\nLEFT JOIN ods_order_details od ON o.订单ID = od.订单ID\nGROUP BY o.订单ID;' },
      { name: 'dwd_order_detail_fact', desc: '订单明细事实表', sql: 'CREATE TABLE dwd_order_detail_fact AS\nSELECT \n  od.*,\n  p.商品名称, p.一级类目, p.二级类目, p.成本,\n  (p.成本 * od.数量) AS 成本金额,\n  (od.金额 - p.成本 * od.数量) AS 毛利,\n  ROUND((od.金额 - p.成本 * od.数量) / od.金额 * 100, 2) AS 毛利率\nFROM ods_order_details od\nLEFT JOIN ods_products p ON od.商品ID = p.商品ID;' },
      { name: 'dim_product', desc: '商品维度表', sql: 'CREATE TABLE dim_product AS\nSELECT \n  p.*,\n  s.店铺名称, s.平台,\n  ROUND((p.售价 - p.成本) / p.售价 * 100, 2) AS 利润率\nFROM ods_products p\nLEFT JOIN ods_stores s ON p.店铺ID = s.店铺ID;' },
      { name: 'dim_store', desc: '店铺维度表', sql: 'CREATE TABLE dim_store AS\nSELECT * FROM ods_stores;' },
      { name: 'dim_user', desc: '用户维度表', sql: 'CREATE TABLE dim_user AS\nSELECT \n  *,\n  CASE \n    WHEN 年龄 <= 25 THEN \'18-25岁\'\n    WHEN 年龄 <= 35 THEN \'26-35岁\'\n    WHEN 年龄 <= 45 THEN \'36-45岁\'\n    WHEN 年龄 <= 55 THEN \'46-55岁\'\n    ELSE \'55岁以上\'\n  END AS 年龄段\nFROM ods_users;' }
    ]
  },
  2: { // DWS层
    tables: [
      { name: 'dws_sales_summary', desc: '销售汇总表', sql: 'CREATE TABLE dws_sales_summary AS\nSELECT \n  订单日期 AS 日期,\n  DATE_FORMAT(订单日期, \'%Y-%m\') AS 年月,\n  平台, 店铺ID, 店铺名称,\n  一级类目, 二级类目,\n  COUNT(订单ID) AS 订单数,\n  COUNT(DISTINCT 用户ID) AS 客户数,\n  SUM(实付金额) AS 销售额,\n  SUM(成本总额) AS 成本,\n  SUM(毛利) AS 毛利,\n  ROUND(SUM(毛利) / SUM(实付金额) * 100, 2) AS 毛利率,\n  ROUND(SUM(实付金额) / COUNT(订单ID), 2) AS 客单价\nFROM dwd_order_fact\nWHERE 订单状态 = \'已完成\'\nGROUP BY 日期, 平台, 店铺ID, 一级类目, 二级类目;' },
      { name: 'dws_inventory_summary', desc: '库存汇总表', sql: 'CREATE TABLE dws_inventory_summary AS\nSELECT \n  p.*,\n  i.变动后库存 AS 当前库存,\n  (p.成本 * i.变动后库存) AS 库存金额_成本,\n  (p.售价 * i.变动后库存) AS 库存金额_售价,\n  SUM(CASE WHEN i.变动类型=\'入库\' THEN i.变动数量 ELSE 0 END) AS 近30天入库量,\n  SUM(CASE WHEN i.变动类型=\'出库\' THEN i.变动数量 ELSE 0 END) AS 近30天出库量\nFROM dim_product p\nLEFT JOIN ods_inventory i ON p.商品ID = i.商品ID\nGROUP BY p.商品ID;' },
      { name: 'dws_promotion_summary', desc: '推广汇总表', sql: 'CREATE TABLE dws_promotion_summary AS\nSELECT \n  pm.日期, pm.平台, pm.店铺ID, pm.商品ID, pm.推广渠道,\n  p.商品名称, p.一级类目, p.二级类目,\n  SUM(pm.推广花费) AS 推广花费,\n  SUM(pm.曝光量) AS 曝光量,\n  SUM(pm.点击量) AS 点击量,\n  ROUND(SUM(pm.点击量) / SUM(pm.曝光量) * 100, 2) AS 点击率,\n  COUNT(DISTINCT o.订单ID) AS 成交订单数,\n  SUM(o.实付金额) AS 成交金额,\n  ROUND(SUM(o.实付金额) / SUM(pm.推广花费), 2) AS ROI\nFROM ods_promotion pm\nLEFT JOIN dim_product p ON pm.商品ID = p.商品ID\nLEFT JOIN dwd_order_fact o ON pm.商品ID = o.商品ID AND pm.日期 = o.订单日期\nGROUP BY pm.日期, pm.平台, pm.商品ID, pm.推广渠道;' },
      { name: 'dws_traffic_summary', desc: '流量汇总表', sql: 'CREATE TABLE dws_traffic_summary AS\nSELECT \n  t.日期, t.店铺ID, t.平台,\n  s.店铺名称,\n  t.访客数, t.浏览量,\n  t.搜索流量, t.推荐流量, t.直接访问,\n  COUNT(o.订单ID) AS 成交订单数,\n  SUM(o.实付金额) AS 成交金额,\n  COUNT(DISTINCT o.用户ID) AS 成交客户数,\n  ROUND(COUNT(DISTINCT o.用户ID) / t.访客数 * 100, 2) AS 访问转化率,\n  ROUND(t.浏览量 / t.访客数, 2) AS 人均浏览页数\nFROM ods_traffic t\nLEFT JOIN dim_store s ON t.店铺ID = s.店铺ID\nLEFT JOIN dwd_order_fact o ON t.店铺ID = o.店铺ID AND t.日期 = o.订单日期\nGROUP BY t.日期, t.店铺ID;' }
    ]
  }
};

// 显示SQL信息
function showSqlInfo(step) {
  const sqlInfoEl = document.getElementById(`sql-info-${step}`);
  const sqlContentEl = document.getElementById(`sql-content-${step}`);
  
  if (sqlInfoEl.style.display === 'none') {
    const info = sqlInfo[step];
    let html = '';
    
    info.tables.forEach(table => {
      html += `
        <div class="sql-table-item">
          <div class="sql-table-name">
            <span class="sql-table-icon">📊</span>
            <span>${table.name}</span>
            <span style="color: #71717a; font-weight: normal; font-size: 12px;">- ${table.desc}</span>
          </div>
          <div class="sql-statement">${highlightSql(table.sql)}</div>
        </div>
      `;
    });
    
    sqlContentEl.innerHTML = html;
    sqlInfoEl.style.display = 'block';
  } else {
    sqlInfoEl.style.display = 'none';
  }
}

// 切换SQL显示
function toggleSql(step) {
  const sqlInfoEl = document.getElementById(`sql-info-${step}`);
  sqlInfoEl.style.display = 'none';
}

// SQL语法高亮
function highlightSql(sql) {
  const keywords = ['CREATE', 'TABLE', 'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LEFT JOIN', 'INNER JOIN', 'ON', 'AS', 'AND', 'OR', 'IN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'ROUND', 'DISTINCT', 'DATE', 'YEAR', 'MONTH', 'DATE_FORMAT'];
  
  let highlighted = sql;
  keywords.forEach(keyword => {
    const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
    highlighted = highlighted.replace(regex, `<span class="sql-keyword">${keyword}</span>`);
  });
  
  return highlighted;
}

// 更新状态时显示SQL按钮
function updateStatusWithSql(step, status, icon, color) {
  updateStatus(step, status, icon, color);
  if (status === '已完成') {
    document.getElementById(`sql-btn-${step}`).style.display = 'inline-block';
  }
}





// 显示清空数据对话框
function showClearDialog() {
  document.getElementById('clear-dialog').style.display = 'flex';
}

// 确认清空数据
async function confirmClear() {
  const clearType = document.querySelector('input[name="clear-type"]:checked').value;
  
  closeDialog('clear-dialog');
  
  // 显示清空类型
  const typeText = clearType === 'all' ? '本地文件和数据库' : 
                   clearType === 'local' ? '本地文件' : '数据库';
  
  // 显示进度对话框
  showProgressDialog(`清空数据 - ${typeText}`);
  appendProgressLog(`开始清空${typeText}...`);
  
  // 获取极速模式选项
  const fastMode = document.getElementById('fast-mode').checked;
  
  try {
    const result = await ipcRenderer.invoke('clear-data', {
      clearType: clearType,
      fastMode: fastMode,
      dbConfig: getDbConfig()
    });
    
    if (result.success) {
      // 重置所有步骤状态
      for (let i = 0; i < 3; i++) {
        updateStatus(i, '未执行', '⚪', '#71717a');
        const sqlBtn = document.getElementById(`sql-btn-${i}`);
        if (sqlBtn) sqlBtn.style.display = 'none';
      }
      
      appendProgressLog('\n操作完成！');
      showToast('数据清空完成！', 'success');
    } else {
      appendProgressLog(`\n[错误] ${result.message}`);
      showToast(`清空失败: ${result.message}`, 'error');
    }
  } catch (error) {
    appendProgressLog(`\n[错误] ${error.message}`);
    showToast(`清空失败: ${error.message}`, 'error');
  }
}

// MySQL 性能优化
async function optimizeMysql() {
  showProgressDialog('MySQL 性能优化检测');
  appendProgressLog('开始检测 MySQL 配置...');
  
  try {
    const result = await ipcRenderer.invoke('optimize-mysql', {
      dbConfig: getDbConfig()
    });
    
    if (result.success) {
      appendProgressLog('\n检测完成！');
      showToast('性能检测完成，请查看日志', 'success');
    } else {
      appendProgressLog(`\n[错误] ${result.message}`);
      showToast(`检测失败: ${result.message}`, 'error');
    }
  } catch (error) {
    appendProgressLog(`\n[错误] ${error.message}`);
    showToast(`检测失败: ${error.message}`, 'error');
  }
}
