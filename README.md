# GBIF 物种数据获取工具

一个基于 Flask 的 Web 应用，用于从 GBIF（全球生物多样性信息设施）获取物种分布数据。

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Flask](https://img.shields.io/badge/Flask-3.0-green)

## 功能特性

- � **GBIF 账号登录** - 支持用户名密码认证
- ⚡ **异步下载** - 使用 GBIF occurrences.download API 进行大规模异步下载
- 📚 **历史复用** - 自动检测并复用 GBIF 账号中已有的历史下载记录
- �� **智能物种识别** - 自动解析物种学名为 GBIF usageKey
- 📊 **批量处理** - 支持同时处理多个物种
- 🔄 **自动去重** - 智能去除重复记录
- 📁 **自定义保存路径** - 可指定输出目录
- 📋 **实时进度** - 显示详细日志和进度百分比
- ⏹️ **任务控制** - 支持随时取消运行中的任务
- 💾 **大文件处理** - 分块处理（50000条/块），避免内存溢出
- 📥 **多种格式导出** - 支持 CSV、Excel、JSON 格式
- 🎯 **标准化输出** - 11列标准格式：物种名、宿主分类、经纬度、国家、省级、年份、来源、个体数、记录类型、备注

## 快速开始

### 本地运行

```bash
pip install -r requirements.txt
python app.py
```

访问 http://localhost:5000

### 使用方法

1. **输入物种名称** - 支持中文名或拉丁学名，多个物种用逗号分隔
2. **设置时间范围**（可选）- 指定采集年份范围
3. **选择输出格式** - CSV / Excel / JSON
4. **输入 GBIF 账号** - 必须填写用户名、密码和邮箱
5. **点击开始获取数据** - 系统自动完成下载、处理、导出

### GBIF 账号说明

- 必须拥有 GBIF 账号才能使用异步下载功能
- 首次使用后，系统会记住账号信息
- 如有历史下载记录，系统会自动复用，无需重复等待

## 部署到 Render

1. 在 GitHub 创建新仓库
2. 将以下文件上传到仓库：
   - `app.py`
   - `index.html`
   - `requirements.txt`
   - `runtime.txt`
   - `Procfile`
   - `README.md`
3. 访问 Render.com 并使用 GitHub 登录
4. 创建新的 Web Service
5. 选择您的仓库
6. 等待部署完成

## 文件说明

| 文件 | 说明 |
|------|------|
| `app.py` | Flask 主程序 |
| `index.html` | Web 前端界面 |
| `requirements.txt` | Python 依赖 |
| `Procfile` | 部署配置 |
| `runtime.txt` | Python 版本 |

## 许可证

MIT
