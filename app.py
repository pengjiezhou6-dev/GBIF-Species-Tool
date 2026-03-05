#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import re
import threading
import requests
import uuid
import zipfile
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pandas as pd
import pycountry
from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import io

try:
    from pygbif import species
    from pygbif import occurrences as occ
except ImportError:
    print("请安装 pygbif 库：pip install pygbif")
    sys.exit(1)


app = Flask(__name__)
CORS(app)

app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

global_logs = []
global_progress = 0
global_species_info = []
global_download_files = []
is_running = False


def format_species_code(scientific_name: str) -> str:
    """格式化学名，保留亚种信息"""
    if not scientific_name or pd.isna(scientific_name):
        return ""
    
    name = str(scientific_name).strip()
    name = re.sub(r'\s+', ' ', name)
    
    parts = name.split()
    if len(parts) >= 2:
        return name
    return name


def convert_country_code(iso2_code: str) -> str:
    """将 ISO2 国家代码转换为 ISO3 格式"""
    if not iso2_code or pd.isna(iso2_code):
        return ""
    
    try:
        country = pycountry.countries.get(alpha_2=str(iso2_code).upper())
        if country:
            return country.alpha_3
    except Exception:
        pass
    return str(iso2_code)


def is_taxon_id(input_str: str) -> bool:
    """判断输入是否为 taxon ID（纯数字）"""
    if not input_str:
        return False
    return input_str.strip().isdigit()


class GBIFAsyncDownloader:
    """GBIF 异步下载器"""
    
    def __init__(self, username: str, password: str, email: str, log_callback=None):
        self.username = username
        self.password = password
        self.email = email
        self.credentials = (username, password)
        self.log_callback = log_callback

    def log(self, message: str):
        if self.log_callback:
            self.log_callback(message)

    def get_usage_keys(self, species_names: List[str]) -> List[int]:
        """获取物种的 usageKeys"""
        usage_keys = []
        for name in species_names:
            try:
                result = species.name_backbone(name)
                if result:
                    usage_info = result.get('usage', {})
                    if isinstance(usage_info, dict) and 'key' in usage_info:
                        usage_keys.append(usage_info['key'])
                        self.log(f"  找到 '{name}' 的 usageKey: {usage_info['key']}")
            except Exception as e:
                self.log(f"  错误：获取 '{name}' 的 usageKey 失败: {e}")
        return usage_keys

    def submit_download_request(self, usage_keys: List[int], year_range: Optional[Tuple[int, int]] = None) -> str:
        """提交异步下载请求"""
        predicates = [
            {"type": "in", "key": "TAXON_KEY", "values": usage_keys},
            {"type": "equals", "key": "HAS_COORDINATE", "value": "true"},
            {"type": "equals", "key": "HAS_GEOSPATIAL_ISSUE", "value": "false"}
        ]
        
        if year_range:
            year_start, year_end = year_range
            predicates.append({
                "type": "and",
                "predicates": [
                    {"type": "greaterThanOrEquals", "key": "YEAR", "value": str(year_start)},
                    {"type": "lessThanOrEquals", "key": "YEAR", "value": str(year_end)}
                ]
            })
        
        request_body = {
            "creator": self.email,
            "notificationAddresses": [self.email],
            "sendNotification": True,
            "format": "SIMPLE_CSV",
            "predicate": {
                "type": "and",
                "predicates": predicates
            }
        }
        
        response = requests.post(
            "https://api.gbif.org/v1/occurrence/download/request",
            json=request_body,
            auth=self.credentials,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 201:
            download_key = response.text.strip()
            self.log(f"  下载请求已提交，Download Key: {download_key}")
            return download_key
        else:
            raise Exception(f"提交下载请求失败: {response.status_code} - {response.text}")

    def check_download_status(self, download_key: str) -> Dict:
        """检查下载状态"""
        response = requests.get(
            f"https://api.gbif.org/v1/occurrence/download/{download_key}",
            auth=self.credentials
        )
        return response.json()

    def download_zip(self, download_url: str, use_auth: bool = True) -> bytes:
        """下载 ZIP 文件"""
        if use_auth:
            response = requests.get(download_url, auth=self.credentials, stream=True, timeout=300)
        else:
            response = requests.get(download_url, stream=True, timeout=300)
        return response.content


def find_matching_download(username: str, password: str, usage_keys: List[int], year_range: Optional[Tuple[int, int]] = None) -> Optional[Dict]:
    """查找匹配的历史下载"""
    try:
        result = occ.download_list(user=username, pwd=password, limit=50)
        for item in result.get('results', []):
            if item.get('status') != 'SUCCEEDED':
                continue
            
            request_info = item.get('request', {})
            predicate = request_info.get('predicate', {})
            
            if predicate.get('type') != 'and':
                continue
            
            found_taxon_keys = set()
            for p in predicate.get('predicates', []):
                if p.get('key') == 'TAXON_KEY' and p.get('type') == 'in':
                    found_taxon_keys = set(p.get('values', []))
            
            if found_taxon_keys == set(usage_keys):
                return item
        return None
    except Exception:
        return None


def process_gbif_zip_bytes(zip_bytes: bytes, output_path: str, host_class_default: str = '', log_callback=None):
    """处理 GBIF ZIP 文件"""
    chunk_size = 50000
    
    FIELD_MAPPING = {
        'species': 'scientificName',
        'longitude': 'decimalLongitude',
        'latitude': 'decimalLatitude',
        'country': 'countryCode',
        'admin1': 'stateProvince',
        'year': 'year',
        'source': None,
        'n_individuals': 'individualCount',
        'host_class': None,
        'obs_type': None,
        'remarks': None
    }
    
    def log(msg):
        if log_callback:
            log_callback(msg)
    
    log(f"开始解压 ZIP 文件...")
    
    zip_buffer = io.BytesIO(zip_bytes)
    with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        if not csv_files:
            return False, "ZIP 中未找到 CSV 文件", 0
        
        csv_file = csv_files[0]
        log(f"找到数据文件: {csv_file}")
        
        all_data = []
        total_records = 0
        
        with zip_file.open(csv_file) as f:
            for chunk in pd.read_csv(f, chunksize=chunk_size, sep='\t', encoding='utf-8', on_bad_lines='skip'):
                total_records += len(chunk)
                log(f"已读取 {total_records} 条记录...")
                
                processed_chunk = []
                for _, row in chunk.iterrows():
                    processed = {}
                    processed['species'] = format_species_code(row.get('scientificName', ''))
                    processed['longitude'] = row.get('decimalLongitude', '')
                    processed['latitude'] = row.get('decimalLatitude', '')
                    
                    country_code = row.get('countryCode', '')
                    processed['country'] = convert_country_code(country_code)
                    
                    processed['admin1'] = row.get('stateProvince', '')
                    processed['year'] = row.get('year', '')
                    processed['source'] = 'GBIF'
                    processed['n_individuals'] = row.get('individualCount', 1)
                    processed['host_class'] = host_class_default
                    processed['obs_type'] = 'occurrence'
                    processed['remarks'] = ''
                    
                    processed_chunk.append(processed)
                
                all_data.extend(processed_chunk)
        
        if not all_data:
            return False, "未获取到有效数据", 0
        
        log(f"共读取 {total_records} 条记录，开始去重...")
        
        final_df = pd.DataFrame(all_data)
        original_count = len(final_df)
        final_df = final_df.drop_duplicates(subset=['species', 'longitude', 'latitude', 'year'], keep='first')
        
        log(f"去重完成：{original_count} -> {len(final_df)} 条记录")
        
        if output_path.endswith('.xlsx'):
            final_df.to_excel(output_path, index=False, engine='openpyxl')
        else:
            final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        log(f"文件已保存: {output_path}")
        
        return True, f"成功处理 {len(final_df)} 条记录", len(final_df)


def export_data(df: pd.DataFrame, output_path: str, output_format: str):
    """导出数据"""
    if output_format == 'xlsx':
        df.to_excel(output_path, index=False, engine='openpyxl')
    elif output_format == 'csv':
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
    elif output_format == 'json':
        df.to_json(output_path, orient='records', force_ascii=False, indent=2)


def process_data(inputs: List[str], year_range: Tuple[int, int], output_mode: str,
                 template_columns: List[str] = None, output_format: str = 'xlsx',
                 host_class_default: str = '', gbif_credentials: Dict = None) -> Tuple[bool, str, List]:
    """处理数据获取任务 - 使用异步下载"""
    global global_logs, global_progress, global_species_info, global_download_files, is_running
    
    try:
        downloader = GBIFAsyncDownloader(
            gbif_credentials['username'],
            gbif_credentials['password'],
            gbif_credentials['email'],
            log_callback=lambda msg: global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        )

        global_download_files = []

        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 正在解析 {len(inputs)} 个物种...")
        usage_keys = downloader.get_usage_keys(inputs)

        if not usage_keys:
            return False, "未找到有效的物种 usageKey", []

        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 成功获取 {len(usage_keys)} 个 usageKeys")

        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 检查历史下载记录...")
        existing_download = find_matching_download(
            gbif_credentials['username'],
            gbif_credentials['password'],
            usage_keys,
            year_range
        )

        if existing_download:
            download_key = existing_download.get('key')
            download_url = existing_download.get('downloadLink')
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 找到匹配的历史下载！Key: {download_key}")
            global_progress = 50
            
            if download_url:
                global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 直接使用历史下载数据")
            else:
                global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 获取下载链接...")
                status = downloader.check_download_status(download_key)
                download_url = status.get('downloadLink')
                if not download_url:
                    return False, "无法获取下载链接", []
        else:
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 未找到匹配的历史下载，提交新申请...")
            download_key = downloader.submit_download_request(usage_keys, year_range)
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 下载申请已提交，Download Key: {download_key}")
            global_progress = 10

            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 等待 GBIF 处理...")

            max_wait_time = 3600
            wait_interval = 10
            elapsed = 0

            while elapsed < max_wait_time:
                if not is_running:
                    return False, "任务已取消", []

                global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 将在 {wait_interval} 秒后检查状态...")

                time.sleep(wait_interval)
                elapsed += wait_interval

                status = downloader.check_download_status(download_key)
                current_status = status.get('status', 'UNKNOWN')

                global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 当前状态: {current_status} (已等待 {elapsed} 秒)")

                global_progress = 10 + min(40, int(elapsed / max_wait_time * 40))

                if current_status == 'SUCCEEDED':
                    global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 下载准备完成！")
                    download_url = status.get('downloadLink')
                    break
                elif current_status == 'FAILED':
                    return False, "GBIF 下载任务失败", []
                elif current_status == 'KILLED':
                    return False, "GBIF 下载任务被取消", []

            if elapsed >= max_wait_time:
                return False, "等待超时，请稍后使用 Download Key 手动查询", []

        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 正在下载数据...")
        
        if not download_url:
            return False, "无法获取下载链接", []

        zip_bytes = downloader.download_zip(download_url)
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ZIP 下载完成，大小: {len(zip_bytes) / 1024 / 1024:.2f} MB")
        global_progress = 60

        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 正在处理数据...")

        date_str = datetime.now().strftime("%Y%m%d")
        species_name = inputs[0] if inputs else "unknown"
        safe_species_name = species_name.replace(' ', '_').replace('/', '_').replace('\\', '_')[:30]
        download_id = download_key if download_key else datetime.now().strftime("%H%M%S")
        output_filename = f"{safe_species_name}_{date_str}_{download_id}.{output_format}"
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)

        success, message, record_count = process_gbif_zip_bytes(
            zip_bytes,
            output_path,
            host_class_default,
            log_callback=lambda msg: global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        )

        if not success:
            return False, message, []

        global_progress = 90

        if output_mode == 'template' and template_columns:
            if output_format == 'xlsx':
                df = pd.read_excel(output_path)
            else:
                df = pd.read_csv(output_path)

            result_df = pd.DataFrame(columns=template_columns)
            for col in template_columns:
                if col in df.columns:
                    result_df[col] = df[col]
                else:
                    result_df[col] = ''

            export_data(result_df, output_path, output_format)
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 模板匹配完成")

        global_download_files.append(output_filename)
        global_progress = 100
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 完成！共 {record_count} 条记录")

        return True, f"成功处理 {record_count} 条记录", global_download_files

    except Exception as e:
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 错误：{str(e)}")
        return False, str(e), []


@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


@app.route('/api/fetch', methods=['POST'])
def fetch():
    """获取数据 API"""
    global is_running, global_logs, global_progress, global_species_info, global_download_files
    
    if is_running:
        return jsonify({'success': False, 'message': '已有任务正在运行'})
    
    species_input = request.form.get('species_input', '').strip()
    if not species_input:
        return jsonify({'success': False, 'message': '请输入物种学名或 ID'})
    
    gbif_username = request.form.get('gbif_username', '').strip()
    gbif_password = request.form.get('gbif_password', '').strip()
    gbif_email = request.form.get('gbif_email', '').strip()
    
    if not gbif_username or not gbif_password or not gbif_email:
        return jsonify({'success': False, 'message': '请填写完整的 GBIF 账号信息'})
    
    try:
        year_start = int(request.form.get('year_start', 2010))
        year_end = int(request.form.get('year_end', 2024))
    except ValueError:
        return jsonify({'success': False, 'message': '年份格式错误'})
    
    if year_start > year_end:
        return jsonify({'success': False, 'message': '起始年份不能大于结束年份'})
    
    output_mode = request.form.get('output_mode', 'default')
    output_format = request.form.get('output_format', 'xlsx')
    host_class_default = request.form.get('host_class_default', '').strip()
    
    template_columns = None
    if output_mode == 'template':
        template_columns_str = request.form.get('template_columns', '')
        if template_columns_str:
            template_columns = [c.strip() for c in template_columns_str.split(',') if c.strip()]
    
    inputs = [s.strip() for s in species_input.split(',') if s.strip()]
    
    is_running = True
    global_logs = []
    global_progress = 0
    global_species_info = []
    global_download_files = []
    
    gbif_credentials = {
        'username': gbif_username,
        'password': gbif_password,
        'email': gbif_email
    }

    def run_task():
        global is_running
        try:
            success, message, files = process_data(
                inputs, (year_start, year_end), output_mode,
                template_columns=template_columns,
                output_format=output_format,
                host_class_default=host_class_default,
                gbif_credentials=gbif_credentials
            )
        finally:
            is_running = False

    thread = threading.Thread(target=run_task)
    thread.start()
    
    return jsonify({
        'success': True,
        'message': '任务已启动，使用 GBIF 异步下载，请等待完成...',
        'species_info': global_species_info
    })


@app.route('/api/logs')
def get_logs():
    """获取日志 API"""
    download_urls = []
    if global_download_files and not is_running:
        download_urls = [f"/api/download/{f}" for f in global_download_files]
    
    return jsonify({
        'logs': global_logs[-50:],
        'progress': global_progress,
        'species_info': global_species_info,
        'is_running': is_running,
        'download_files': global_download_files,
        'download_urls': download_urls
    })


@app.route('/api/cancel', methods=['POST'])
def cancel_task():
    """取消当前运行的任务"""
    global is_running, global_logs
    
    if not is_running:
        return jsonify({'success': False, 'message': '没有正在运行的任务'})
    
    is_running = False
    global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 任务已被用户取消")
    
    return jsonify({'success': True, 'message': '任务已取消'})


@app.route('/api/download-history')
def get_download_history():
    """获取用户的 GBIF 下载历史"""
    username = request.args.get('username', '').strip()
    password = request.args.get('password', '').strip()
    
    if not username or not password:
        return jsonify({'success': False, 'message': '请提供 GBIF 账号信息'})
    
    try:
        result = occ.download_list(user=username, pwd=password, limit=20)
        downloads = []
        for item in result.get('results', []):
            downloads.append({
                'key': item.get('key', ''),
                'status': item.get('status', ''),
                'created': item.get('created', ''),
                'size': item.get('size', 0),
                'downloadLink': item.get('downloadLink', '')
            })
        return jsonify({'success': True, 'downloads': downloads})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/download/<filename>')
def download(filename):
    """下载文件 API"""
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    return jsonify({'success': False, 'message': '文件不存在'}), 404


@app.route('/api/download-all')
def download_all():
    """一键下载所有文件（ZIP打包）"""
    global global_download_files
    
    if not global_download_files:
        return jsonify({'success': False, 'message': '没有可下载的文件'}), 404
    
    first_file = global_download_files[0]
    parts = first_file.replace('.xlsx', '').replace('.csv', '').split('_')
    species_name = parts[0] if len(parts) > 0 else 'species'
    date_str = parts[1] if len(parts) > 1 else datetime.now().strftime("%Y%m%d")
    download_id = parts[2] if len(parts) > 2 else datetime.now().strftime("%H%M%S")
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename in global_download_files:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            if os.path.exists(file_path):
                zip_file.write(file_path, filename)
    
    zip_buffer.seek(0)
    
    zip_filename = f"{species_name}_{date_str}_{download_id}.zip"
    
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=zip_filename
    )


if __name__ == '__main__':
    print("=" * 50)
    print("GBIF 物种数据获取工具 Web 版 v2.4")
    print("=" * 50)
    print("启动服务器...")
    print("访问地址：http://localhost:5000")
    print("按 Ctrl+C 停止服务器")
    print("=" * 50)
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
