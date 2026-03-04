# GBIF 物种数据获取工具

一个基于 Flask 的 Web 应用，用于从 GBIF（全球生物多样性信息设施）获取物种分布数据。

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Flask](https://img.shields.io/badge/Flask-3.0-green)

## 功能特性

- 🔍 智能识别物种学名和 GBIF ID
- 📊 支持批量处理多个物种
- 🔄 自动去重
- 📁 自定义保存路径
- 📋 实时进度日志
- 📥 CSV 文件下载

## 快速开始

### 本地运行

```bash
pip install -r requirements.txt
python app.py
```

访问 http://localhost:5000

## 部署到 Render

1. 在 GitHub 创建新仓库
2. 将所有文件上传到仓库
3. 访问 Render.com 并使用 GitHub 登录
4. 创建新的 Web Service
5. 选择您的仓库
6. 等待部署完成

## 文件说明

| 文件 | 说明 |
|------|------|
| `app.py` | Flask 主程序 |
| `templates/index.html` | Web 界面 |
| `requirements.txt` | Python 依赖 |
| `Procfile` | 部署配置 |
| `runtime.txt` | Python 版本 |

## 许可证

MIT
