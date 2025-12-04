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
  
  // 绑定企业体量变化事件 - 自动配置店铺
  document.querySelectorAll('input[name="business-scale"]').forEach(radio => {
    radio.addEventListener('change', (e) => {
      applyScaleStores(e.target.value);
    });
  });
  
  // 绑定时间跨度变化事件
  document.getElementById('time-span-days').addEventListener('change', updateGenerateEstimate);
  document.getElementById('time-span-days').addEventListener('input', updateGenerateEstimate);
  
  // 初始化时应用默认体量的店铺配置
  const defaultScale = document.querySelector('input[name="business-scale"]:checked');
  if (defaultScale && Object.keys(platformStores).length === 0) {
    applyScaleStores(defaultScale.value);
  }
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
      
      // 加载企业体量
      if (config.businessScale) {
        const scaleRadios = document.querySelectorAll('input[name="business-scale"]');
        scaleRadios.forEach(radio => {
          if (radio.value === config.businessScale) radio.checked = true;
        });
      }
      
      // 加载时间跨度
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
    businessScale: document.querySelector('input[name="business-scale"]:checked')?.value || '小型企业',
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

// 记录步骤开始时间
function startStepTimer(step) {
  stepStartTime[step] = Date.now();
}

// 停止计时并显示耗时
function stopStepTimer(step) {
  if (stepStartTime[step]) {
    const elapsed = Math.floor((Date.now() - stepStartTime[step]) / 1000);
    const minutes = Math.floor(elapsed / 60);
    const secs = elapsed % 60;
    const timeText = minutes > 0 ? `${minutes}分${secs}秒` : `${secs}秒`;
    stepStartTime[step] = null;
    return timeText;
  }
  return '';
}

// 企业体量配置（含店铺数量范围和平台分配）
const BUSINESS_SCALES = {
  '微型企业': { 
    dailyTraffic: 250, multiplier: 0.5, storeRange: [3, 5],
    platforms: { '天猫': 2, '京东': 2, '抖音': 1 }
  },
  '小型企业': { 
    dailyTraffic: 1500, multiplier: 1.0, storeRange: [6, 10],
    platforms: { '天猫': 3, '京东': 3, '抖音': 2, '拼多多': 2 }
  },
  '中型企业': { 
    dailyTraffic: 6000, multiplier: 2.0, storeRange: [10, 20],
    platforms: { '天猫': 4, '京东': 4, '抖音': 3, '拼多多': 3, '快手': 2, '小红书': 2 }
  },
  '大型企业': { 
    dailyTraffic: 40000, multiplier: 5.0, storeRange: [20, 50],
    platforms: { '天猫': 8, '京东': 8, '抖音': 6, '拼多多': 6, '快手': 4, '小红书': 4, '微信': 2 }
  },
  '超大型企业': { 
    dailyTraffic: 200000, multiplier: 10.0, storeRange: [50, 100],
    platforms: { '天猫': 15, '京东': 15, '抖音': 10, '拼多多': 10, '快手': 6, '小红书': 6, '微信': 4 }
  }
};

// 根据企业体量自动配置店铺
function applyScaleStores(businessScale) {
  const config = BUSINESS_SCALES[businessScale];
  if (!config || !config.platforms) return;
  
  // 清空现有配置
  platformStores = {};
  
  // 先取消所有平台勾选
  platforms.forEach(platform => {
    const checkbox = document.getElementById(`platform-${platform}`);
    if (checkbox) {
      checkbox.checked = false;
      const storeList = document.getElementById(`stores-${platform}`);
      if (storeList) storeList.style.display = 'none';
    }
  });
  
  // 根据配置勾选平台并设置店铺数
  Object.entries(config.platforms).forEach(([platform, count]) => {
    const checkbox = document.getElementById(`platform-${platform}`);
    const countInput = document.getElementById(`count-${platform}`);
    
    if (checkbox && countInput) {
      checkbox.checked = true;
      countInput.value = count;
      platformStores[platform] = generateStoreNames(platform, count);
      renderStoreList(platform);
      const storeList = document.getElementById(`stores-${platform}`);
      if (storeList) storeList.style.display = 'block';
    }
  });
  
  updateGenerateEstimate();
}

// 根据店铺数量自动推荐企业体量
function getRecommendedScale(storeCount) {
  if (storeCount <= 2) return '微型企业';
  if (storeCount <= 5) return '小型企业';
  if (storeCount <= 10) return '中型企业';
  if (storeCount <= 20) return '大型企业';
  return '超大型企业';
}

// 检查店铺数量与企业体量是否匹配
function checkScaleMatch(businessScale, storeCount) {
  const config = BUSINESS_SCALES[businessScale];
  if (!config) return true;
  const [min, max] = config.storeRange;
  return storeCount >= min && storeCount <= max;
}

// 更新生成预估
function updateGenerateEstimate() {
  const scaleRadio = document.querySelector('input[name="business-scale"]:checked');
  let businessScale = scaleRadio ? scaleRadio.value : '小型企业';
  const timeSpanDays = parseInt(document.getElementById('time-span-days').value) || 365;
  
  // 计算店铺数
  let storeCount = 0;
  Object.values(platformStores).forEach(stores => {
    storeCount += stores.length;
  });
  
  if (storeCount === 0) {
    document.getElementById('estimate-section').style.display = 'none';
    return;
  }
  
  // 检查店铺数量与企业体量是否匹配，不匹配则自动推荐
  if (!checkScaleMatch(businessScale, storeCount)) {
    const recommended = getRecommendedScale(storeCount);
    // 自动选中推荐的体量
    const recommendedRadio = document.querySelector(`input[name="business-scale"][value="${recommended}"]`);
    if (recommendedRadio) {
      recommendedRadio.checked = true;
      businessScale = recommended;
    }
  }
  
  const config = BUSINESS_SCALES[businessScale] || BUSINESS_SCALES['小型企业'];
  
  // 计算预估值
  const dailyTraffic = config.dailyTraffic * storeCount;
  const totalTraffic = dailyTraffic * timeSpanDays;
  const totalClicks = Math.floor(totalTraffic * 0.03);  // 3%点击率
  const estimatedOrders = Math.floor(totalClicks * 0.05);  // 5%转化率
  const estimatedGmv = estimatedOrders * 500;  // 客单价500
  const monthlyGmv = estimatedGmv / (timeSpanDays / 30);
  const estimatedUsers = Math.floor(totalClicks / 10);
  
  // 显示预估
  document.getElementById('estimate-section').style.display = 'block';
  document.getElementById('estimate-traffic').textContent = formatNumber(totalTraffic);
  document.getElementById('estimate-orders').textContent = formatNumber(estimatedOrders) + ' 单';
  document.getElementById('estimate-gmv').textContent = formatMoney(monthlyGmv) + '/月';
  document.getElementById('estimate-users').textContent = formatNumber(estimatedUsers) + ' 人';
}

function formatNumber(num) {
  if (num >= 100000000) return (num / 100000000).toFixed(1) + '亿';
  if (num >= 10000) return (num / 10000).toFixed(1) + '万';
  return num.toLocaleString();
}

function formatMoney(num) {
  if (num >= 100000000) return (num / 100000000).toFixed(1) + '亿';
  if (num >= 10000) return (num / 10000).toFixed(0) + '万';
  return num.toLocaleString() + '元';
}

function showEstimate() {}
function stopEstimate(step) {
  const timeText = stopStepTimer(step);
  if (timeText) {
    const statusEl = document.getElementById(`status-${step}`);
    if (statusEl && statusEl.textContent.includes('已完成')) {
      statusEl.textContent = `✅ 已完成 (${timeText})`;
    }
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

  const businessScale = document.querySelector('input[name="business-scale"]:checked').value;
  const timeSpanDays = parseInt(document.getElementById('time-span-days').value);
  const mainCategory = document.querySelector('input[name="main-category"]:checked').value;

  // 先保存配置
  await saveConfig();

  closeDialog('generate-dialog');
  updateStatus(0, '执行中...', '🔄', '#ed8936');
  currentProcessIds[0] = 'generate-ods';
  startStepTimer(0);
  showStepProgress(0);
  appendStepLog(0, `开始生成ODS层数据（${businessScale}）...`);

  try {
    const result = await ipcRenderer.invoke('generate-ods', {
      platformStores,
      businessScale,
      timeSpanDays,
      mainCategory,
      dbConfig: globalDbConfig
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
  startStepTimer(0);
  
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
  currentProcessIds[1] = 'generate-dwd';
  startStepTimer(1);
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
  currentProcessIds[2] = 'generate-dws';
  startStepTimer(2);
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

// 生成ADS层（业务宽表）
async function generateAds() {
  updateStatus(3, '执行中...', '🔄', '#ed8936');
  currentProcessIds[3] = 'generate-ads';
  startStepTimer(3);
  showStepProgress(3);
  appendStepLog(3, '开始构建ADS层业务宽表...');

  try {
    const result = await ipcRenderer.invoke('generate-ads', {
      mode: 'full',
      dbConfig: getDbConfig()
    });
    
    if (result.success) {
      updateStatus(3, '已完成', '✅', '#48bb78');
      appendStepLog(3, '\n[完成] ADS层业务宽表构建完成！');
      showToast('ADS层业务宽表构建完成！', 'success');
    }
  } catch (error) {
    updateStatus(3, '失败', '❌', '#f56565');
    appendStepLog(3, `\n[错误] ${error.message}`);
    showToast(`构建失败: ${error.message}`, 'error');
  } finally {
    hideStepProgress(3);
    stopEstimate(3);
    currentProcessIds[3] = null;
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
  2: null,  // DWS
  3: null   // ADS
};

// 倒计时定时器
let countdownTimers = {
  0: null,
  1: null,
  2: null,
  3: null
};

// 步骤开始时间
let stepStartTime = {
  0: null,
  1: null,
  2: null,
  3: null
};

// 步骤进度信息
let stepProgress = {
  0: { current: 0, total: 0 },
  1: { current: 0, total: 0 },
  2: { current: 0, total: 0 },
  3: { current: 0, total: 0 }
};

// 监听日志消息
ipcRenderer.on('log-message', (event, message) => {
  // 在控制台输出所有日志
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

// 添加步骤日志（显示所有步骤和耗时）
function appendStepLog(step, message) {
  const logContainer = document.getElementById(`progress-log-${step}`);
  if (!logContainer) return;
  
  // 显示所有重要信息
  const isImportant = 
    message.includes('[') ||
    message.includes('✗') || 
    message.includes('✓') ||
    message.includes('开始') ||
    message.includes('删除') ||
    message.includes('创建') ||
    message.includes('插入') ||
    message.includes('加载') ||
    message.includes('关联') ||
    message.includes('构建') ||
    message.includes('转换') ||
    message.includes('生成') ||
    message.includes('完成') ||
    message.includes('失败') ||
    message.includes('秒') ||
    message.includes('行');
  
  if (!isImportant) return;
  
  const logLine = document.createElement('div');
  logLine.className = 'log-line';
  
  // 根据消息类型设置样式
  if (message.includes('[错误]') || message.includes('✗') || message.includes('失败')) {
    logLine.classList.add('log-error');
  } else if (message.includes('✓') || message.includes('完成')) {
    logLine.classList.add('log-success');
  } else if (message.includes('[进度]') || message.includes('%')) {
    logLine.classList.add('log-progress');
  } else if (message.includes('【') || message.includes('[')) {
    logLine.classList.add('log-info');
  }
  
  logLine.textContent = message;
  logContainer.appendChild(logLine);
  
  // 自动滚动到底部
  logContainer.parentElement.scrollTop = logContainer.parentElement.scrollHeight;
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
  const keywords = ['CREATE', 'TABLE', 'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'ON', 'AS', 'GROUP', 'BY', 'ORDER', 'LIMIT', 'INSERT', 'UPDATE', 'DELETE', 'AND', 'OR', 'NOT', 'NULL', 'PRIMARY', 'KEY', 'INDEX', 'VARCHAR', 'INT', 'DECIMAL', 'DATE', 'DATETIME', 'SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'DISTINCT', 'ROUND'];
  
  let highlighted = sql;
  keywords.forEach(keyword => {
    const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
    highlighted = highlighted.replace(regex, `<span class="sql-keyword">${keyword}</span>`);
  });
  
  return highlighted;
}

// 更新状态并显示SQL按钮
function updateStatusWithSql(step, status, icon, color) {
  updateStatus(step, status, icon, color);
  const sqlBtn = document.getElementById(`sql-btn-${step}`);
  if (sqlBtn && status === '已完成') {
    sqlBtn.style.display = 'inline-block';
  }
}

// 显示数据库状态检测对话框
function showDbStatusDialog() {
  document.getElementById('db-status-dialog').style.display = 'flex';
  runDbStatus();
}

// 运行数据库状态检测
async function runDbStatus() {
  const logContainer = document.getElementById('db-status-log');
  logContainer.innerHTML = '<div class="log-line">⏳ 正在检测数据库状态...</div>';
  
  // 临时监听日志
  const logHandler = (event, message) => {
    const logLine = document.createElement('div');
    logLine.className = 'log-line';
    
    if (message.includes('❌') || message.includes('✗') || message.includes('[错误]')) {
      logLine.classList.add('log-error');
    } else if (message.includes('✓') || message.includes('[完成]')) {
      logLine.classList.add('log-success');
    } else if (message.includes('⚠️') || message.includes('[警告]')) {
      logLine.classList.add('log-warning');
    } else if (message.includes('===') || message.includes('【')) {
      logLine.classList.add('log-info');
    }
    
    logLine.textContent = message;
    logContainer.appendChild(logLine);
    logContainer.parentElement.scrollTop = logContainer.parentElement.scrollHeight;
  };
  
  ipcRenderer.on('log-message', logHandler);
  
  try {
    await ipcRenderer.invoke('db-status', {
      dbConfig: getDbConfig()
    });
  } catch (error) {
    const logLine = document.createElement('div');
    logLine.className = 'log-line log-error';
    logLine.textContent = `检测失败: ${error.message}`;
    logContainer.appendChild(logLine);
  } finally {
    ipcRenderer.removeListener('log-message', logHandler);
  }
}

// 停止所有查询
async function killAllQueries() {
  if (!confirm('确定要停止当前数据库的所有查询吗？\n\n这将终止所有正在执行的SQL语句。')) {
    return;
  }
  
  const logContainer = document.getElementById('db-status-log');
  logContainer.innerHTML = '<div class="log-line">⏳ 正在停止所有查询...</div>';
  
  const logHandler = (event, message) => {
    const logLine = document.createElement('div');
    logLine.className = 'log-line';
    if (message.includes('✗') || message.includes('失败')) {
      logLine.classList.add('log-error');
    } else if (message.includes('✓') || message.includes('成功')) {
      logLine.classList.add('log-success');
    }
    logLine.textContent = message;
    logContainer.appendChild(logLine);
    logContainer.parentElement.scrollTop = logContainer.parentElement.scrollHeight;
  };
  
  ipcRenderer.on('log-message', logHandler);
  
  try {
    await ipcRenderer.invoke('kill-all-queries', { dbConfig: getDbConfig() });
    showToast('查询已停止', 'success');
  } catch (error) {
    showToast(`停止失败: ${error.message}`, 'error');
  } finally {
    ipcRenderer.removeListener('log-message', logHandler);
  }
}

// 显示优化表结构对话框
function showOptimizedTables() {
  closeDialog('optimize-dialog');
  
  if (confirm('将创建优化后的表结构（支持分区、压缩存储）\n\n注意：如果表已存在，将会被删除重建！\n\n是否继续？')) {
    createOptimizedTables();
  }
}

// 创建优化表结构
async function createOptimizedTables() {
  showProgressDialog('创建优化表结构');
  appendProgressLog('开始创建优化表结构...');
  
  try {
    const result = await ipcRenderer.invoke('create-optimized-tables', {
      dbConfig: getDbConfig()
    });
    
    if (result.success) {
      appendProgressLog('\n[完成] 优化表结构创建完成！');
      showToast('优化表结构创建完成', 'success');
    }
  } catch (error) {
    appendProgressLog(`\n[错误] ${error.message}`);
    showToast(`创建失败: ${error.message}`, 'error');
  }
}

// 显示数据验证视图
function showVerifyView() {
  document.getElementById('steps-container').style.display = 'none';
  document.getElementById('verify-view').style.display = 'block';
  // 延迟一下再开始验证，确保DOM已渲染
  setTimeout(() => {
    startVerifyInView();
  }, 100);
}

// 隐藏数据验证视图
function hideVerifyView() {
  document.getElementById('verify-view').style.display = 'none';
  document.getElementById('steps-container').style.display = 'block';
}

// 在视图中开始验证
async function startVerifyInView() {
  const contentContainer = document.getElementById('verify-content');
  contentContainer.innerHTML = '<div style="text-align: center; padding: 50px; color: #a1a1aa;"><div class="loading">⏳ 正在验证数据一致性...</div></div>';
  
  // 获取当前配置
  let businessScale = '小型企业';
  try {
    const configResult = await ipcRenderer.invoke('load-generate-config');
    if (configResult.success && configResult.config) {
      businessScale = configResult.config.businessScale || '小型企业';
    }
  } catch (error) {
    console.log('无法加载配置，使用默认值');
  }
  
  let isFirstMessage = true;
  let messageCount = 0;
  
  // 临时监听日志
  const logHandler = (event, message) => {
    messageCount++;
    console.log(`收到消息 #${messageCount}:`, message.substring(0, 100));
    
    // 第一条消息时清空加载提示
    if (isFirstMessage) {
      contentContainer.innerHTML = '';
      isFirstMessage = false;
    }
    
    // 如果是HTML内容，直接插入
    if (message.trim().startsWith('<')) {
      contentContainer.insertAdjacentHTML('beforeend', message);
    } else {
      const logLine = document.createElement('div');
      logLine.className = 'log-line';
      
      if (message.includes('❌') || message.includes('✗') || message.includes('[错误]')) {
        logLine.classList.add('log-error');
      } else if (message.includes('✅') || message.includes('✓')) {
        logLine.classList.add('log-success');
      } else if (message.includes('⚠️')) {
        logLine.classList.add('log-progress');
      } else if (message.includes('===') || message.includes('【')) {
        logLine.classList.add('log-info');
      }
      
      logLine.textContent = message;
      contentContainer.appendChild(logLine);
    }
    contentContainer.scrollTop = contentContainer.scrollHeight;
  };
  
  ipcRenderer.on('log-message', logHandler);
  
  try {
    console.log('开始验证，配置:', { businessScale });
    const result = await ipcRenderer.invoke('verify-data-consistency', {
      dbConfig: getDbConfig(),
      dataDir: 'data/ods',
      businessScale: businessScale
    });
    
    console.log('验证结果:', result);
    console.log('总共收到消息数:', messageCount);
    
    if (result.success) {
      showToast('数据验证完成！', 'success');
    } else {
      contentContainer.innerHTML = `<div style="text-align: center; padding: 50px; color: #f87171; font-size: 16px;">❌ 验证失败: ${result.message || '未知错误'}</div>`;
    }
  } catch (error) {
    console.error('验证异常:', error);
    contentContainer.innerHTML = `<div style="text-align: center; padding: 50px; color: #f87171; font-size: 16px;">❌ 验证失败: ${error.message}</div>`;
    showToast('验证失败', 'error');
  } finally {
    ipcRenderer.removeListener('log-message', logHandler);
  }
}

// 显示数据验证对话框（保留兼容）
function showVerifyDialog() {
  showVerifyView();
}

// 开始数据验证（保留兼容）
async function startVerify() {
  await startVerifyInView();
}

// 应用MySQL优化配置
async function applyMysqlOptimization() {
  closeDialog('optimize-dialog');
  
  if (confirm('将应用MySQL性能优化配置\n\n包括：缓冲池、连接数、日志等参数\n\n是否继续？')) {
    showProgressDialog('应用MySQL优化');
    appendProgressLog('开始应用优化配置...');
    
    try {
      const result = await ipcRenderer.invoke('optimize-mysql', {
        dbConfig: getDbConfig()
      });
      
      if (result.success) {
        appendProgressLog('\n[完成] 优化配置已应用！');
        showToast('优化配置已应用', 'success');
      }
    } catch (error) {
      appendProgressLog(`\n[错误] ${error.message}`);
      showToast(`应用失败: ${error.message}`, 'error');
    }
  }
}

// 旧的优化函数（保持兼容）
async function optimizeMysql() {
  showOptimizeMenu();
}

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
