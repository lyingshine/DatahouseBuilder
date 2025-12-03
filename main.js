const { app, BrowserWindow, ipcMain } = require('electron');
const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs');

let mainWindow;
let pythonProcess;
let currentProcessType = null; // 记录当前进程类型

// 获取资源路径（开发环境和打包后都能正确工作）
const isDev = !app.isPackaged;
const resourcesPath = isDev ? __dirname : process.resourcesPath;
const appPath = isDev ? __dirname : path.join(resourcesPath, 'app.asar.unpacked');

// 获取用户数据目录
const userDataPath = app.getPath('userData');
const configPath = path.join(userDataPath, '数据库信息');

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1000,
    height: 700,
    minWidth: 900,
    minHeight: 650,
    frame: false,
    transparent: false,
    backgroundColor: '#1a1a1a',
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false,
      enableRemoteModule: true
    },
    icon: path.join(resourcesPath, 'build/icon.png')
  });

  mainWindow.loadFile('renderer/index.html');

  // 开发模式下打开开发者工具
  if (process.argv.includes('--dev')) {
    mainWindow.webContents.openDevTools();
  }

  mainWindow.on('closed', () => {
    mainWindow = null;
    if (pythonProcess) {
      pythonProcess.kill();
    }
  });
}

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});

// 窗口控制
ipcMain.on('window-minimize', () => {
  if (mainWindow) mainWindow.minimize();
});

ipcMain.on('window-maximize', () => {
  if (mainWindow) {
    if (mainWindow.isMaximized()) {
      mainWindow.unmaximize();
    } else {
      mainWindow.maximize();
    }
  }
});

ipcMain.on('window-close', () => {
  if (mainWindow) mainWindow.close();
});

// IPC 通信处理
ipcMain.handle('generate-ods', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/generate_data.py');
    pythonProcess = spawn('python', [scriptPath, JSON.stringify(config)], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    currentProcessType = 'generate-ods';

    let output = '';
    let error = '';

    pythonProcess.stdout.on('data', (data) => {
      const text = data.toString('utf8');
      output += text;
      event.sender.send('log-message', text);
    });

    pythonProcess.stderr.on('data', (data) => {
      const text = data.toString('utf8');
      error += text;
      event.sender.send('log-message', `错误: ${text}`);
    });

    pythonProcess.on('close', (code) => {
      pythonProcess = null;
      currentProcessType = null;
      if (code === 0) {
        resolve({ success: true, message: 'ODS层数据生成完成！' });
      } else {
        reject({ success: false, message: error || '生成失败' });
      }
    });
  });
});

ipcMain.handle('generate-dwd', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/transform_dwd.py');
    pythonProcess = spawn('python', [scriptPath, JSON.stringify(config || {})], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    currentProcessType = 'generate-dwd';

    pythonProcess.stdout.on('data', (data) => {
      event.sender.send('log-message', data.toString('utf8'));
    });

    pythonProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    pythonProcess.on('close', (code) => {
      pythonProcess = null;
      currentProcessType = null;
      if (code === 0) {
        resolve({ success: true, message: 'DWD层事实表构建完成！' });
      } else {
        reject({ success: false, message: '构建失败' });
      }
    });
  });
});



ipcMain.handle('generate-dws', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/transform_dws.py');
    pythonProcess = spawn('python', [scriptPath, JSON.stringify(config || {})], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    currentProcessType = 'generate-dws';

    pythonProcess.stdout.on('data', (data) => {
      event.sender.send('log-message', data.toString('utf8'));
    });

    pythonProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    pythonProcess.on('close', (code) => {
      pythonProcess = null;
      currentProcessType = null;
      if (code === 0) {
        resolve({ success: true, message: 'DWS层数据转换完成！' });
      } else {
        reject({ success: false, message: '转换失败' });
      }
    });
  });
});

ipcMain.handle('generate-ads', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/transform_ads.py');
    pythonProcess = spawn('python', [scriptPath, JSON.stringify(config || {})], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    currentProcessType = 'generate-ads';

    pythonProcess.stdout.on('data', (data) => {
      event.sender.send('log-message', data.toString('utf8'));
    });

    pythonProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    pythonProcess.on('close', (code) => {
      pythonProcess = null;
      currentProcessType = null;
      if (code === 0) {
        resolve({ success: true, message: 'ADS层业务宽表构建完成！' });
      } else {
        reject({ success: false, message: '构建失败' });
      }
    });
  });
});

ipcMain.handle('preview-data', async (event, layer) => {
  const fs = require('fs').promises;
  const dataPath = path.join(appPath, `data/${layer}`);
  
  try {
    const files = await fs.readdir(dataPath);
    const csvFiles = files.filter(f => f.endsWith('.csv'));
    
    if (csvFiles.length === 0) {
      return { success: false, message: '暂无数据文件' };
    }

    // 读取前3个文件的前10行
    const previews = [];
    for (let i = 0; i < Math.min(3, csvFiles.length); i++) {
      const filePath = path.join(dataPath, csvFiles[i]);
      const content = await fs.readFile(filePath, 'utf-8');
      const lines = content.split('\n').slice(0, 11).join('\n');
      previews.push({ file: csvFiles[i], content: lines });
    }

    return { success: true, data: previews };
  } catch (error) {
    return { success: false, message: error.message };
  }
});

ipcMain.handle('import-ods', async (event, config) => {
  return new Promise((resolve, reject) => {
    const fs = require('fs');
    const dataPath = path.join(appPath, 'data/ods');
    
    try {
      // 确保目录存在
      if (!fs.existsSync(dataPath)) {
        fs.mkdirSync(dataPath, { recursive: true });
      }
      
      // 复制文件到data/ods目录
      let copiedCount = 0;
      config.files.forEach(filePath => {
        const fileName = path.basename(filePath);
        const destPath = path.join(dataPath, fileName);
        
        // 读取并验证文件
        const content = fs.readFileSync(filePath, 'utf-8');
        const lines = content.split('\n');
        
        if (lines.length < 2) {
          event.sender.send('log-message', `警告: ${fileName} 文件为空或格式不正确`);
          return;
        }
        
        // 复制文件
        fs.copyFileSync(filePath, destPath);
        copiedCount++;
        event.sender.send('log-message', `已导入: ${fileName}`);
      });
      
      if (copiedCount > 0) {
        resolve({ success: true, message: `成功导入 ${copiedCount} 个文件` });
      } else {
        reject({ success: false, message: '没有有效的文件被导入' });
      }
    } catch (error) {
      event.sender.send('log-message', `错误: ${error.message}`);
      reject({ success: false, message: error.message });
    }
  });
});

ipcMain.handle('load-to-database', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/load_to_database.py');
    pythonProcess = spawn('python', [scriptPath, JSON.stringify(config)], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    currentProcessType = 'load-to-database';

    pythonProcess.stdout.on('data', (data) => {
      event.sender.send('log-message', data.toString('utf8'));
    });

    pythonProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    pythonProcess.on('close', (code) => {
      pythonProcess = null;
      currentProcessType = null;
      if (code === 0) {
        resolve({ success: true, message: '数据加载完成！' });
      } else {
        reject({ success: false, message: '数据加载失败' });
      }
    });
  });
});

ipcMain.handle('clear-data', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/clear_data.py');
    pythonProcess = spawn('python', [scriptPath, JSON.stringify(config)], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    currentProcessType = 'clear-data';

    pythonProcess.stdout.on('data', (data) => {
      event.sender.send('log-message', data.toString('utf8'));
    });

    pythonProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    pythonProcess.on('close', (code) => {
      pythonProcess = null;
      currentProcessType = null;
      if (code === 0) {
        resolve({ success: true, message: '数据清空完成！' });
      } else {
        reject({ success: false, message: '数据清空失败' });
      }
    });
  });
});

// 保存数据库配置文件
ipcMain.handle('save-db-config', async (event, dbConfig) => {
  const fsPromises = require('fs').promises;
  
  try {
    // 确保用户数据目录存在
    if (!fs.existsSync(userDataPath)) {
      fs.mkdirSync(userDataPath, { recursive: true });
    }
    
    const content = `数据库类型：mysql
数据库地址：${dbConfig.host}
端口：${dbConfig.port}
数据库名：${dbConfig.database}
用户名：${dbConfig.user}
密码：${dbConfig.password}
`;
    
    await fsPromises.writeFile(configPath, content, 'utf-8');
    return { success: true, message: '数据库配置已保存' };
  } catch (error) {
    return { success: false, message: error.message };
  }
});

// 读取数据库配置文件
ipcMain.handle('load-db-config', async () => {
  const fsPromises = require('fs').promises;
  
  try {
    const content = await fsPromises.readFile(configPath, 'utf-8');
    const lines = content.split('\n');
    const config = {};
    
    lines.forEach(line => {
      if (line.includes('：')) {
        const [key, value] = line.split('：');
        const trimmedValue = value.trim();
        
        if (key.includes('数据库地址')) config.host = trimmedValue;
        else if (key.includes('端口')) config.port = parseInt(trimmedValue);
        else if (key.includes('数据库名')) config.database = trimmedValue;
        else if (key.includes('用户名')) config.user = trimmedValue;
        else if (key.includes('密码')) config.password = trimmedValue;
      }
    });
    
    return { success: true, config };
  } catch (error) {
    // 文件不存在时返回默认配置
    return {
      success: true,
      config: {
        host: 'localhost',
        port: 3306,
        database: 'datas',
        user: 'root',
        password: ''
      }
    };
  }
});

// 测试数据库连接
ipcMain.handle('test-db-connection', async (event, dbConfig) => {
  return new Promise((resolve) => {
    const scriptPath = path.join(appPath, 'scripts/test_connection.py');
    const testProcess = spawn('python', [scriptPath, JSON.stringify(dbConfig)], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    let output = '';
    
    testProcess.stdout.on('data', (data) => {
      output += data.toString('utf8');
    });

    testProcess.on('close', (code) => {
      if (code === 0) {
        resolve({ success: true, connected: true });
      } else {
        resolve({ success: true, connected: false, message: output });
      }
    });

    // 超时处理
    setTimeout(() => {
      testProcess.kill();
      resolve({ success: true, connected: false, message: '连接超时' });
    }, 5000);
  });
});

// 停止进程
ipcMain.handle('stop-process', async (event, processId) => {
  if (pythonProcess && currentProcessType) {
    try {
      // Windows 使用 taskkill 强制终止进程树
      if (process.platform === 'win32') {
        const { execSync } = require('child_process');
        try {
          execSync(`taskkill /pid ${pythonProcess.pid} /T /F`, { stdio: 'ignore' });
        } catch (e) {
          // 进程可能已经结束
        }
      } else {
        pythonProcess.kill('SIGINT');
      }
      pythonProcess = null;
      currentProcessType = null;
      return { success: true, message: '进程已停止' };
    } catch (error) {
      return { success: false, message: `停止失败: ${error.message}` };
    }
  }
  return { success: false, message: '没有运行中的进程' };
});

// 保存生成配置
ipcMain.handle('save-generate-config', async (event, config) => {
  const fsPromises = require('fs').promises;
  const configFilePath = path.join(appPath, 'config.json');
  
  try {
    await fsPromises.writeFile(configFilePath, JSON.stringify(config, null, 2), 'utf-8');
    return { success: true, message: '配置已保存' };
  } catch (error) {
    return { success: false, message: error.message };
  }
});

// 加载生成配置
ipcMain.handle('load-generate-config', async () => {
  const fsPromises = require('fs').promises;
  const configFilePath = path.join(appPath, 'config.json');
  
  try {
    const content = await fsPromises.readFile(configFilePath, 'utf-8');
    const config = JSON.parse(content);
    return { success: true, config };
  } catch (error) {
    // 文件不存在时返回默认配置
    return {
      success: true,
      config: {
        platformStores: {
          '京东': ['京东旗舰店1号', '京东旗舰店2号', '京东旗舰店3号', '京东旗舰店4号'],
          '天猫': ['天猫旗舰店1号', '天猫旗舰店2号', '天猫旗舰店3号', '天猫旗舰店4号'],
          '抖音': ['抖音旗舰店1号', '抖音旗舰店2号', '抖音旗舰店3号', '抖音旗舰店4号'],
          '快手': ['快手旗舰店1号', '快手旗舰店2号', '快手旗舰店3号', '快手旗舰店4号'],
          '微信': ['微信旗舰店1号', '微信旗舰店2号', '微信旗舰店3号', '微信旗舰店4号'],
          '小红书': ['小红书旗舰店1号', '小红书旗舰店2号', '小红书旗舰店3号', '小红书旗舰店4号'],
          '拼多多': ['拼多多旗舰店1号', '拼多多旗舰店2号', '拼多多旗舰店3号', '拼多多旗舰店4号']
        },
        numOrders: 20000,
        timeSpanDays: 365,
        mainCategory: 'bicycle'
      }
    };
  }
});

// MySQL 性能优化检测
ipcMain.handle('optimize-mysql', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/optimize_mysql.py');
    const optimizeProcess = spawn('python', [scriptPath, JSON.stringify(config)], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    optimizeProcess.stdout.on('data', (data) => {
      event.sender.send('log-message', data.toString('utf8'));
    });

    optimizeProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    optimizeProcess.on('close', (code) => {
      if (code === 0) {
        resolve({ success: true, message: '性能检测完成' });
      } else {
        reject({ success: false, message: '性能检测失败' });
      }
    });
  });
});

// 数据库状态检测
ipcMain.handle('db-status', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/db_status.py');
    const statusProcess = spawn('python', [scriptPath, JSON.stringify(config)], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    statusProcess.stdout.on('data', (data) => {
      const lines = data.toString('utf8').split('\n');
      lines.forEach(line => {
        if (line.trim()) {
          event.sender.send('log-message', line);
        }
      });
    });

    statusProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    statusProcess.on('close', (code) => {
      resolve({ success: code === 0 });
    });
  });
});

// 停止所有查询
ipcMain.handle('kill-all-queries', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/db_status.py');
    const killConfig = { ...config, action: 'kill' };
    const killProcess = spawn('python', [scriptPath, JSON.stringify(killConfig)], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    killProcess.stdout.on('data', (data) => {
      const lines = data.toString('utf8').split('\n');
      lines.forEach(line => {
        if (line.trim()) {
          event.sender.send('log-message', line);
        }
      });
    });

    killProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    killProcess.on('close', (code) => {
      resolve({ success: code === 0 });
    });
  });
});

// 创建优化表结构
ipcMain.handle('create-optimized-tables', async (event, config) => {
  return new Promise((resolve, reject) => {
    const sqlPath = path.join(appPath, 'sql/create_tables_optimized.sql');
    const fs = require('fs');
    
    if (!fs.existsSync(sqlPath)) {
      reject({ success: false, message: 'SQL文件不存在' });
      return;
    }
    
    const scriptPath = path.join(appPath, 'scripts/execute_sql.py');
    const executeProcess = spawn('python', [scriptPath, JSON.stringify({
      ...config,
      sqlFile: sqlPath
    })], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    executeProcess.stdout.on('data', (data) => {
      event.sender.send('log-message', data.toString('utf8'));
    });

    executeProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    executeProcess.on('close', (code) => {
      if (code === 0) {
        resolve({ success: true, message: '优化表结构创建完成' });
      } else {
        reject({ success: false, message: '创建失败' });
      }
    });
  });
});

// 数据一致性验证
ipcMain.handle('verify-data-consistency', async (event, config) => {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(appPath, 'scripts/verify_data_consistency.py');
    const verifyProcess = spawn('python', [scriptPath, JSON.stringify(config)], {
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    verifyProcess.stdout.on('data', (data) => {
      event.sender.send('log-message', data.toString('utf8'));
    });

    verifyProcess.stderr.on('data', (data) => {
      event.sender.send('log-message', `错误: ${data.toString('utf8')}`);
    });

    verifyProcess.on('close', (code) => {
      if (code === 0) {
        resolve({ success: true, message: '数据验证完成' });
      } else {
        reject({ success: false, message: '验证失败' });
      }
    });
  });
});
