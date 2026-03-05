#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import re
import threading
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


class GBIFDataFetcher:
    """GBIF 数据获取类"""
    
    def __init__(self, log_callback=None):
        self.log_callback = log_callback
        self.request_delay = 0.5
    
    def log(self, message: str):
        """输出日志"""
        if self.log_callback:
            self.log_callback(message)
        else:
            print(message)
    
    def get_species_info_by_id(self, taxon_key: int) -> Optional[Dict]:
        """通过 taxonKey 获取物种详细信息"""
        try:
            result = species.name_usage(key=taxon_key)
            if result:
                info = {
                    'key': result.get('key', taxon_key),
                    'scientificName': result.get('scientificName', ''),
                    'canonicalName': result.get('canonicalName', ''),
                    'rank': result.get('rank', ''),
                    'status': result.get('status', ''),
                    'kingdom': result.get('kingdom', ''),
                    'phylum': result.get('phylum', ''),
                    'class': result.get('class', ''),
                    'order': result.get('order', ''),
                    'family': result.get('family', ''),
                    'genus': result.get('genus', ''),
                    'species': result.get('species', '')
                }
                return info
            return None
        except Exception as e:
            self.log(f"  错误：获取 ID {taxon_key} 的物种信息时发生异常：{str(e)}")
            return None
    
    def get_usage_key(self, species_name: str) -> Optional[int]:
        """通过学名获取 usageKey"""
        try:
            result = species.name_backbone(species_name)
            if result and 'usage' in result:
                usage_key = result['usage'].get('key')
                if usage_key:
                    self.log(f"  找到物种 '{species_name}' 的 usageKey: {usage_key}")
                    return int(usage_key)
            self.log(f"  警告：未找到物种 '{species_name}' 的 usageKey")
            return None
        except Exception as e:
            self.log(f"  错误：查询物种 '{species_name}' 时发生异常：{str(e)}")
            return None
    
    def get_species_info_by_name(self, species_name: str) -> Optional[Dict]:
        """通过学名获取物种信息（包含 usageKey）"""
        try:
            result = species.name_backbone(species_name)
            if result and 'usage' in result:
                usage_info = result['usage']
                classification = result.get('classification', [])
                
                class_dict = {c.get('rank', '').lower(): c.get('name', '') for c in classification}
                
                info = {
                    'key': usage_info.get('key', ''),
                    'scientificName': usage_info.get('name', ''),
                    'canonicalName': usage_info.get('canonicalName', ''),
                    'rank': usage_info.get('rank', ''),
                    'status': usage_info.get('status', ''),
                    'kingdom': class_dict.get('kingdom', ''),
                    'phylum': class_dict.get('phylum', ''),
                    'class': class_dict.get('class', ''),
                    'order': class_dict.get('order', ''),
                    'family': class_dict.get('family', ''),
                    'genus': class_dict.get('genus', ''),
                    'species': usage_info.get('canonicalName', '')
                }
                return info
            return None
        except Exception as e:
            self.log(f"  错误：查询物种 '{species_name}' 时发生异常：{str(e)}")
            return None
    
    def resolve_input(self, input_str: str) -> Tuple[Optional[int], Optional[Dict]]:
        """智能解析输入"""
        input_str = input_str.strip()
        
        if is_taxon_id(input_str):
            taxon_key = int(input_str)
            self.log(f"  检测到输入为 ID: {taxon_key}")
            species_info = self.get_species_info_by_id(taxon_key)
            if species_info:
                self.log(f"  物种名称：{species_info.get('scientificName', '未知')}")
                return taxon_key, species_info
            else:
                self.log(f"  警告：无法获取 ID {taxon_key} 的物种信息")
                return taxon_key, {'key': taxon_key, 'scientificName': f'ID:{taxon_key}'}
        else:
            self.log(f"  检测到输入为学名：{input_str}")
            species_info = self.get_species_info_by_name(input_str)
            if species_info:
                usage_key = species_info.get('key')
                if usage_key:
                    self.log(f"  找到对应 ID: {usage_key}")
                    return int(usage_key), species_info
            return None, None
    
    def fetch_occurrences(self, usage_key: int, year_range: Tuple[int, int]) -> List[Dict]:
        """获取物种出现记录"""
        all_records = []
        offset = 0
        page_size = 300
        
        year_start, year_end = year_range
        
        self.log(f"  开始获取数据 (年份范围：{year_start}-{year_end})...")
        
        while True:
            try:
                params = {
                    'taxonKey': usage_key,
                    'year': f'{year_start},{year_end}',
                    'limit': page_size,
                    'offset': offset,
                    'hasCoordinate': True
                }
                
                result = occ.search(**params)
                
                if result and 'results' in result:
                    records = result['results']
                    if not records:
                        self.log(f"  已获取所有可用数据，共 {len(all_records)} 条记录")
                        break
                    
                    all_records.extend(records)
                    self.log(f"  已获取 {len(all_records)} 条记录...")
                    
                    if len(records) < page_size:
                        break
                    
                    offset += page_size
                    time.sleep(self.request_delay)
                else:
                    break
                    
            except Exception as e:
                self.log(f"  警告：获取数据时发生错误：{str(e)}")
                time.sleep(2)
                continue
        
        return all_records


class DataProcessor:
    """数据处理类"""
    
    FIELD_MAPPING = {
        'species': 'scientificName',
        'longitude': 'decimalLongitude',
        'latitude': 'decimalLatitude',
        'country': 'countryCode',
        'admin1': 'stateProvince',
        'year': 'year',
        'source': None,
        'event_type': None,
        'n_individuals': 'individualCount',
        'host_class': None,
        'remarks': None
    }
    
    def __init__(self, log_callback=None):
        self.log_callback = log_callback
    
    def log(self, message: str):
        if self.log_callback:
            self.log_callback(message)
    
    def process_records(self, records: List[Dict]) -> pd.DataFrame:
        """处理原始记录，进行字段映射和去重"""
        if not records:
            return pd.DataFrame()
        
        self.log(f"  开始处理 {len(records)} 条记录...")
        
        processed_data = []
        
        for record in records:
            processed = {}
            
            processed['species'] = format_species_code(record.get('scientificName', ''))
            processed['longitude'] = record.get('decimalLongitude', '')
            processed['latitude'] = record.get('decimalLatitude', '')
            
            country_code = record.get('countryCode', '')
            processed['country'] = convert_country_code(country_code)
            
            processed['admin1'] = record.get('stateProvince', '')
            
            year_val = record.get('year', '')
            processed['year'] = year_val if year_val else ''
            
            processed['source'] = 'GBIF'
            processed['event_type'] = 'occurrence'
            
            individual_count = record.get('individualCount', '')
            if individual_count and str(individual_count).isdigit():
                processed['n_individuals'] = int(individual_count)
            else:
                processed['n_individuals'] = 1
            
            processed['host_class'] = ''
            processed['remarks'] = ''
            
            processed_data.append(processed)
        
        df = pd.DataFrame(processed_data)
        
        original_count = len(df)
        df = df.drop_duplicates(subset=['species', 'longitude', 'latitude', 'year'], keep='first')
        deduplicated_count = len(df)
        
        if original_count > deduplicated_count:
            self.log(f"  自动去重：移除了 {original_count - deduplicated_count} 条重复记录")
        
        self.log(f"  数据处理完成，共 {len(df)} 条有效记录")
        
        return df
    
    def apply_template(self, df: pd.DataFrame, template_path: str) -> pd.DataFrame:
        """应用用户自定义模板"""
        try:
            if template_path.endswith('.xlsx'):
                template_df = pd.read_excel(template_path, nrows=0)
            else:
                template_df = pd.read_csv(template_path, nrows=0)
            
            template_columns = list(template_df.columns)
            self.log(f"  检测到模板列：{template_columns}")
            
            result_df = pd.DataFrame(columns=template_columns)
            
            for col in template_columns:
                if col in df.columns:
                    result_df[col] = df[col]
                else:
                    result_df[col] = ''
            
            return result_df
            
        except Exception as e:
            self.log(f"  警告：应用模板失败，使用默认格式：{str(e)}")
            return df
    
    def get_standard_columns(self) -> List[str]:
        """获取标准输出列名"""
        return list(self.FIELD_MAPPING.keys())


def process_data(inputs: List[str], year_range: Tuple[int, int], 
                 output_mode: str, template_file=None, save_path: str = None) -> Tuple[bool, str, Optional[str]]:
    """处理数据获取任务"""
    global global_logs, global_progress, global_species_info
    
    try:
        fetcher = GBIFDataFetcher(log_callback=lambda msg: global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"))
        processor = DataProcessor(log_callback=lambda msg: global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"))
        
        all_data = []
        total_inputs = len(inputs)
        
        for idx, input_str in enumerate(inputs):
            if not is_running:
                return False, "任务已取消", None
            
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] \n[{idx + 1}/{total_inputs}] 正在处理：{input_str}")
            global_progress = int((idx / total_inputs) * 80)
            
            usage_key, species_info = fetcher.resolve_input(input_str)
            
            if species_info:
                global_species_info.append(species_info)
            
            if usage_key:
                records = fetcher.fetch_occurrences(usage_key, year_range)
                
                if records:
                    df = processor.process_records(records)
                    if not df.empty:
                        all_data.append(df)
            
            time.sleep(0.5)
        
        if not all_data:
            return False, "未获取到任何数据", None
        
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] \n正在合并数据...")
        final_df = pd.concat(all_data, ignore_index=True)
        
        if output_mode == 'template' and template_file:
            template_path = os.path.join(app.config['UPLOAD_FOLDER'], template_file.filename)
            template_file.save(template_path)
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 正在应用模板...")
            final_df = processor.apply_template(final_df, template_path)
        else:
            standard_cols = processor.get_standard_columns()
            final_df = final_df[standard_cols]
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        first_input = inputs[0].strip()
        if first_input.isdigit():
            filename = f"ID{first_input}_点位数据_{timestamp}.csv"
        else:
            first_name = first_input.split()[0] if first_input.split() else "species"
            filename = f"{first_name}_点位数据_{timestamp}.csv"
        
        # 确定保存路径
        if save_path and os.path.isdir(save_path):
            output_dir = save_path
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 使用自定义保存路径：{save_path}")
        else:
            output_dir = app.config['UPLOAD_FOLDER']
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 使用默认保存路径：{output_dir}")
        
        output_path = os.path.join(output_dir, filename)
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        global_progress = 100
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] \n完成！共保存 {len(final_df)} 条记录")
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 文件保存路径：{output_path}")
        
        return True, f"成功保存 {len(final_df)} 条记录", output_path
        
    except Exception as e:
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] \n发生错误：{str(e)}")
        return False, str(e), None


@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


@app.route('/api/fetch', methods=['POST'])
def fetch():
    """获取数据 API"""
    global is_running, global_logs, global_progress, global_species_info
    
    if is_running:
        return jsonify({'success': False, 'message': '已有任务正在运行'})
    
    species_input = request.form.get('species_input', '').strip()
    if not species_input:
        return jsonify({'success': False, 'message': '请输入物种学名或 ID'})
    
    try:
        year_start = int(request.form.get('year_start', 2010))
        year_end = int(request.form.get('year_end', 2024))
    except ValueError:
        return jsonify({'success': False, 'message': '年份格式错误'})
    
    if year_start > year_end:
        return jsonify({'success': False, 'message': '起始年份不能大于结束年份'})
    
    output_mode = request.form.get('output_mode', 'default')
    template_file = request.files.get('template') if 'template' in request.files else None
    save_path = request.form.get('save_path', '').strip()
    
    inputs = [s.strip() for s in species_input.split(',') if s.strip()]
    
    is_running = True
    global_logs = []
    global_progress = 0
    global_species_info = []
    
    def run_task():
        global is_running
        success, message, output_path = process_data(inputs, (year_start, year_end), output_mode, template_file, save_path)
        is_running = False
        
        if success and output_path:
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 下载链接：/api/download/{os.path.basename(output_path)}")
    
    thread = threading.Thread(target=run_task)
    thread.start()
    
    return jsonify({
        'success': True, 
        'message': '任务已启动，请稍候...',
        'species_info': global_species_info
    })


@app.route('/api/logs')
def get_logs():
    """获取日志 API"""
    return jsonify({
        'logs': global_logs[-50:],
        'progress': global_progress,
        'species_info': global_species_info,
        'is_running': is_running
    })


@app.route('/api/download/<filename>')
def download(filename):
    """下载文件 API"""
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    return jsonify({'success': False, 'message': '文件不存在'}), 404


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
