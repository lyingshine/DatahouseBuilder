const { createApp } = Vue;
const { ipcRenderer } = require('electron');

createApp({
  data() {
    return {
      // 当前视图
      currentView: 'steps', // 'steps' | 'verify'
      
      // 数据库配置
      dbConfig: {
        host: 'localhost',
        port: 3306,
        database: 'datas',
        user: 'root',
        password: ''
      },
      dbStatus: {
        connected: false,
        text: '未连接'
      },
      
      // 验证相关
      verifyContent: '',
      verifyLoading: false,
      
      // 步骤状态
      steps: [
        { id: 0, title: 'ODS层（原始数据层）', desc: '生成模拟数据或导入数据到数据库', status: '未执行', color: '#48bb78', running: false },
        { id: 1, title: 'DWD层（明细数据层）', desc: '在数据库中通过SQL转换，构建事实表和维度表', status: '未执行', color: '#667eea', running: false },
        { id: 2, title: 'DWS层（汇总数据层）', desc: '在数据库中通过SQL聚合，构建业务汇总表', status: '未执行', color: '#ed8936', running: false },
        { id: 3, title: 'ADS层（应用数据层）', desc: '构建业务宽表，包含日报、平台汇总、店铺排行等', status: '未执行', color: '#e53e3e', running: false }
      ],
      
      // 对话框
      showDbConfigDialog: false,
      showGenerateDialog: false,
      showClearDialog: false,
      
      // 生成配置
      generateConfig: {
        businessScale: '小型企业',
        timeSpanDays: 365,
        mainCategory: 'bicycle'
      },
      
      // 清空配置
      clearConfig: {
        clearType: 'all',
        fastMode: false
      },
      
      // Toast通知
      toasts: []
    };
  },
  
  mounted() {
    this.loadDbConfig();
    this.updateDbStatus();
    this.setupIpcListeners();
    
    // 每30秒检测一次数据库连接
    setInterval(() => {
      this.updateDbStatus();
    }, 30000);
  },
  
  methods: {
    // 加载数据库配置
    async loadDbConfig() {
      try {
        const result = await ipcRenderer.invoke('load-db-config');
        if (result.success && result.config) {
          this.dbConfig = result.config;
          this.updateDbStatus();
        }
      } catch (error) {
        console.error('加载数据库配置失败:', error);
      }
    },
    
    // 更新数据库状态
    async updateDbStatus() {
      try {
        const config = {
          host: this.dbConfig.host,
          port: this.dbConfig.port,
          database: this.dbConfig.database,
          user: this.dbConfig.user,
          password: this.dbConfig.password
        };
        const result = await ipcRenderer.invoke('test-db-connection', config);
        this.dbStatus.connected = result.connected;
        this.dbStatus.text = result.connected ? '已连接' : '未连接';
      } catch (error) {
        this.dbStatus.connected = false;
        this.dbStatus.text = '连接失败';
      }
    },
    
    // 设置IPC监听
    setupIpcListeners() {
      ipcRenderer.on('log-message', (event, message) => {
        if (this.currentView === 'verify') {
          if (message.trim().startsWith('<')) {
            this.verifyContent += message;
          } else {
            const logLine = `<div class="log-line">${this.escapeHtml(message)}</div>`;
            this.verifyContent += logLine;
          }
          this.$nextTick(() => {
            const container = this.$refs.verifyContent;
            if (container) {
              container.scrollTop = container.scrollHeight;
            }
          });
        }
      });
    },
    
    // 转义HTML
    escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    },
    
    // 显示验证视图
    async showVerifyView() {
      this.currentView = 'verify';
      this.verifyContent = '<div style="text-align: center; padding: 50px; color: #a1a1aa;">⏳ 正在验证数据一致性...</div>';
      this.verifyLoading = true;
      
      await this.$nextTick();
      await this.startVerify();
    },
    
    // 开始验证
    async startVerify() {
      try {
        let businessScale = '小型企业';
        try {
          const configResult = await ipcRenderer.invoke('load-generate-config');
          if (configResult.success && configResult.config) {
            businessScale = configResult.config.businessScale || '小型企业';
          }
        } catch (error) {
          console.log('无法加载配置，使用默认值');
        }
        
        this.verifyContent = '';
        
        const config = {
          host: this.dbConfig.host,
          port: this.dbConfig.port,
          database: this.dbConfig.database,
          user: this.dbConfig.user,
          password: this.dbConfig.password
        };
        const result = await ipcRenderer.invoke('verify-data-consistency', {
          dbConfig: config,
          dataDir: 'data/ods',
          businessScale: businessScale
        });
        
        if (result.success) {
          this.showToast('数据验证完成！', 'success');
        } else {
          this.verifyContent = `<div style="text-align: center; padding: 50px; color: #f87171;">❌ 验证失败: ${result.message || '未知错误'}</div>`;
        }
      } catch (error) {
        this.verifyContent = `<div style="text-align: center; padding: 50px; color: #f87171;">❌ 验证失败: ${error.message}</div>`;
        this.showToast('验证失败', 'error');
      } finally {
        this.verifyLoading = false;
      }
    },
    
    // 返回主界面
    hideVerifyView() {
      this.currentView = 'steps';
    },
    
    // 显示提示
    showToast(message, type = 'info') {
      const toast = {
        id: Date.now(),
        message,
        type,
        show: false
      };
      this.toasts.push(toast);
      
      this.$nextTick(() => {
        toast.show = true;
      });
      
      setTimeout(() => {
        toast.show = false;
        setTimeout(() => {
          const index = this.toasts.indexOf(toast);
          if (index > -1) {
            this.toasts.splice(index, 1);
          }
        }, 300);
      }, 3000);
    },
    
    // 移除Toast
    removeToast(toast) {
      toast.show = false;
      setTimeout(() => {
        const index = this.toasts.indexOf(toast);
        if (index > -1) {
          this.toasts.splice(index, 1);
        }
      }, 300);
    },
    
    // 窗口控制
    minimizeWindow() {
      ipcRenderer.send('window-minimize');
    },
    
    maximizeWindow() {
      ipcRenderer.send('window-maximize');
    },
    
    closeWindow() {
      ipcRenderer.send('window-close');
    },
    
    // 显示数据库配置
    showDbConfig() {
      this.showDbConfigDialog = true;
    },
    
    // 关闭数据库配置
    closeDbConfig() {
      this.showDbConfigDialog = false;
    },
    
    // 保存数据库配置
    async saveDbConfig() {
      try {
        // 转换为普通对象，避免 Vue 响应式对象序列化问题
        const config = {
          host: this.dbConfig.host,
          port: this.dbConfig.port,
          database: this.dbConfig.database,
          user: this.dbConfig.user,
          password: this.dbConfig.password
        };
        const result = await ipcRenderer.invoke('save-db-config', config);
        if (result.success) {
          this.showToast('数据库配置已保存', 'success');
          this.closeDbConfig();
          this.updateDbStatus();
        } else {
          this.showToast(`保存失败: ${result.message}`, 'error');
        }
      } catch (error) {
        this.showToast(`保存失败: ${error.message}`, 'error');
      }
    },
    
    // 显示生成配置
    showGenerate() {
      this.showGenerateDialog = true;
    },
    
    // 关闭生成配置
    closeGenerate() {
      this.showGenerateDialog = false;
    },
    
    // 开始生成
    async startGenerate() {
      this.closeGenerate();
      const step = this.steps[0];
      step.running = true;
      step.status = '执行中...';
      
      try {
        const config = {
          host: this.dbConfig.host,
          port: this.dbConfig.port,
          database: this.dbConfig.database,
          user: this.dbConfig.user,
          password: this.dbConfig.password
        };
        
        const result = await ipcRenderer.invoke('generate-ods', {
          platformStores: {},
          businessScale: this.generateConfig.businessScale,
          timeSpanDays: this.generateConfig.timeSpanDays,
          mainCategory: this.generateConfig.mainCategory,
          dbConfig: config
        });
        
        if (result.success) {
          step.status = '已完成';
          this.showToast('ODS数据生成完成', 'success');
        } else {
          step.status = '失败';
          this.showToast(`生成失败: ${result.message}`, 'error');
        }
      } catch (error) {
        step.status = '失败';
        this.showToast(`生成失败: ${error.message}`, 'error');
      } finally {
        step.running = false;
      }
    },
    
    // 显示清空对话框
    showClear() {
      this.showClearDialog = true;
    },
    
    // 关闭清空对话框
    closeClear() {
      this.showClearDialog = false;
    },
    
    // 确认清空
    async confirmClear() {
      this.closeClear();
      
      try {
        const config = {
          host: this.dbConfig.host,
          port: this.dbConfig.port,
          database: this.dbConfig.database,
          user: this.dbConfig.user,
          password: this.dbConfig.password
        };
        
        const result = await ipcRenderer.invoke('clear-data', {
          clearType: this.clearConfig.clearType,
          fastMode: this.clearConfig.fastMode,
          dbConfig: config
        });
        
        if (result.success) {
          // 重置所有步骤状态
          this.steps.forEach(step => {
            step.status = '未执行';
            step.running = false;
          });
          this.showToast('数据清空完成', 'success');
        } else {
          this.showToast(`清空失败: ${result.message}`, 'error');
        }
      } catch (error) {
        this.showToast(`清空失败: ${error.message}`, 'error');
      }
    },
    
    // 执行步骤
    async executeStep(stepId) {
      const step = this.steps[stepId];
      step.running = true;
      step.status = '执行中...';
      
      try {
        const config = {
          host: this.dbConfig.host,
          port: this.dbConfig.port,
          database: this.dbConfig.database,
          user: this.dbConfig.user,
          password: this.dbConfig.password
        };
        
        let result;
        switch(stepId) {
          case 0:
            // ODS层 - 需要先显示配置对话框
            this.showGenerate();
            break;
          case 1:
            result = await ipcRenderer.invoke('generate-dwd', {
              mode: 'full',
              dbConfig: config
            });
            break;
          case 2:
            result = await ipcRenderer.invoke('generate-dws', {
              mode: 'full',
              dbConfig: config
            });
            break;
          case 3:
            result = await ipcRenderer.invoke('generate-ads', {
              mode: 'full',
              dbConfig: config
            });
            break;
        }
        
        if (result && result.success) {
          step.status = '已完成';
          this.showToast(`${step.title}执行完成`, 'success');
        }
      } catch (error) {
        step.status = '失败';
        this.showToast(`执行失败: ${error.message}`, 'error');
      } finally {
        step.running = false;
      }
    },
    
    // 获取步骤状态图标
    getStepIcon(step) {
      if (step.running) return '🔄';
      if (step.status === '已完成') return '✅';
      if (step.status === '失败') return '❌';
      return '⚪';
    }
  }
}).mount('#app');
