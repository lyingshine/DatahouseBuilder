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
        resolve({ success: true, message: 'DWD层数据转换完成！' });
      } else {
        reject({ success: false, message: '转换失败' });
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
      pythonProcess.kill('SIGTERM');
      pythonProcess = null;
      currentProcessType = null;
      return { success: true, message: '进程已停止' };
    } catch (error) {
      return { success: false, message: `停止失败: ${error.message}` };
    }
  }
  return { success: false, message: '没有运行中的进程' };
});
