#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import re
import json
import threading
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pandas as pd
import pycountry
from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS

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
global_template_info = None
is_running = False

FIELD_MAPPING_KEYWORDS = {
    'species': ['species', '物种', '学名', 'scientificname', 'species_name', 'name', 'speciesname', 'scientific_name'],
    'longitude': ['longitude', '经度', 'lng', 'decimallongitude', 'lon', 'long', 'longitude_dec'],
    'latitude': ['latitude', '纬度', 'lat', 'decimallatitude', 'latitude_dec'],
    'country': ['country', '国家', 'countrycode', 'nation', 'country_code', 'nationcode'],
    'admin1': ['admin1', '省份', 'stateprovince', 'state', 'region', '行政区', 'province', 'admin_1'],
    'year': ['year', '年份', 'year_collected', '年', 'collection_year', 'year_col'],
    'source': ['source', '来源', 'data_source', 'datasource', '数据来源'],
    'obs_type': ['obs_type', '事件类型', 'eventtype', 'record_type', 'observation_type'],
    'n_individuals': ['n_individuals', '个体数', 'count', 'individualcount', 'number', 'individuals', '个体数量'],
    'host_class': ['host_class', '宿主类别', 'host', 'hostclass', '宿主'],
    'remarks': ['remarks', '备注', 'notes', 'comment', 'note', 'comments']
}


def match_column_name(template_col: str) -> Optional[str]:
    """智能匹配模板列名到标准字段名"""
    if not template_col or pd.isna(template_col):
        return None
    col_str = str(template_col).strip()
    if not col_str or col_str.startswith('Unnamed'):
        return None
    
    col_lower = col_str.lower().replace('_', '').replace('-', '').replace(' ', '')
    
    for standard_field, keywords in FIELD_MAPPING_KEYWORDS.items():
        for keyword in keywords:
            keyword_clean = keyword.lower().strip().replace('_', '').replace('-', '').replace(' ', '')
            if keyword_clean == col_lower:
                return standard_field
    
    for standard_field, keywords in FIELD_MAPPING_KEYWORDS.items():
        for keyword in keywords:
            keyword_clean = keyword.lower().strip().replace('_', '').replace('-', '').replace(' ', '')
            if len(keyword_clean) >= 3:
                if keyword_clean in col_lower or col_lower in keyword_clean:
                    return standard_field
    
    return None


def parse_template_file(template_path: str) -> Tuple[List[str], str]:
    """解析模板文件，返回列名列表和格式类型"""
    ext = os.path.splitext(template_path)[1].lower()
    
    if ext == '.xlsx':
        template_df = pd.read_excel(template_path, nrows=0)
        columns = list(template_df.columns)
    elif ext == '.csv':
        template_df = pd.read_csv(template_path, nrows=0, encoding='utf-8-sig')
        columns = list(template_df.columns)
    elif ext == '.json':
        with open(template_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list) and len(data) > 0:
                columns = list(data[0].keys())
            elif isinstance(data, dict):
                columns = list(data.keys())
            else:
                columns = []
    else:
        raise ValueError(f"不支持的文件格式：{ext}")
    
    valid_columns = []
    for col in columns:
        col_str = str(col).strip()
        if col_str and not col_str.startswith('Unnamed'):
            valid_columns.append(col)
    
    return valid_columns, ext.lstrip('.')


def format_species_code(scientific_name: str) -> str:
    """格式化学名为 '属名_种名' 格式"""
    if not scientific_name or pd.isna(scientific_name):
        return ""
    
    name = str(scientific_name).strip()
    name = re.sub(r'\s+', '_', name)
    
    return name


def determine_host_class(record: Dict, default_value: str = '') -> str:
    """根据记录信息判断 host_class"""
    dom_keywords = ['farm', 'captive', 'domestic', 'market', 'zoo', '农场', '市场', '圈养', '养殖', '饲养', '牧场', '动物园']
    wild_keywords = ['wild', 'nature', 'forest', 'reserve', 'wilderness', '野外', '自然保护', '森林', '荒野', '野生']
    
    search_fields = ['locality', 'locationRemarks', 'habitat', 'eventRemarks', 'stateProvince', 'verbatimLocality']
    
    for field in search_fields:
        text = str(record.get(field, '')).lower()
        for keyword in dom_keywords:
            if keyword.lower() in text:
                return 'dom'
        for keyword in wild_keywords:
            if keyword.lower() in text:
                return 'wild'
    
    return default_value


def determine_obs_type(record: Dict) -> str:
    """根据记录信息判断 obs_type"""
    locality = str(record.get('locality', '')).lower()
    state_province = str(record.get('stateProvince', '')).lower()
    combined = locality + ' ' + state_province
    
    farm_keywords = ['farm', '农场', '牧场', '养殖场']
    for keyword in farm_keywords:
        if keyword in combined:
            return 'farm'
    
    centroid_keywords = ['center', 'centre', 'central', '行政中心', '市中心', '县城']
    for keyword in centroid_keywords:
        if keyword in combined:
            return 'centroid'
    
    return 'occurrence'


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
        'obs_type': None,
        'n_individuals': 'individualCount',
        'host_class': None,
        'remarks': None
    }
    
    def __init__(self, log_callback=None, host_class_default: str = ''):
        self.log_callback = log_callback
        self.host_class_default = host_class_default
    
    def log(self, message: str):
        if self.log_callback:
            self.log_callback(message)
    
    def process_records(self, records: List[Dict]) -> pd.DataFrame:
        """处理原始记录，进行字段映射和去重"""
        if not records:
            return pd.DataFrame()
        
        self.log(f"  开始处理 {len(records)} 条记录...")
        
        processed_data = []
        skipped_count = 0
        
        for record in records:
            longitude = record.get('decimalLongitude', '')
            latitude = record.get('decimalLatitude', '')
            
            if not longitude or not latitude or pd.isna(longitude) or pd.isna(latitude):
                skipped_count += 1
                continue
            
            try:
                longitude = float(longitude)
                latitude = float(latitude)
            except (ValueError, TypeError):
                skipped_count += 1
                continue
            
            processed = {}
            
            processed['species'] = format_species_code(record.get('scientificName', ''))
            processed['longitude'] = longitude
            processed['latitude'] = latitude
            
            country_code = record.get('countryCode', '')
            processed['country'] = convert_country_code(country_code)
            
            processed['admin1'] = record.get('stateProvince', '') or ''
            
            year_val = record.get('year', '')
            if year_val:
                try:
                    year_int = int(year_val)
                    if 1000 <= year_int <= 9999:
                        processed['year'] = year_int
                    else:
                        processed['year'] = ''
                except (ValueError, TypeError):
                    processed['year'] = ''
            else:
                processed['year'] = ''
            
            processed['source'] = 'GBIF'
            processed['obs_type'] = determine_obs_type(record)
            
            individual_count = record.get('individualCount', '')
            if individual_count and str(individual_count).isdigit():
                processed['n_individuals'] = int(individual_count)
            else:
                processed['n_individuals'] = 1
            
            processed['host_class'] = determine_host_class(record, self.host_class_default)
            processed['remarks'] = ''
            
            processed_data.append(processed)
        
        if skipped_count > 0:
            self.log(f"  剔除 {skipped_count} 条无效坐标记录")
        
        if not processed_data:
            self.log(f"  无有效记录")
            return pd.DataFrame()
        
        df = pd.DataFrame(processed_data)
        
        original_count = len(df)
        df = df.drop_duplicates(subset=['species', 'longitude', 'latitude', 'year'], keep='first')
        deduplicated_count = len(df)
        
        if original_count > deduplicated_count:
            self.log(f"  自动去重：移除了 {original_count - deduplicated_count} 条重复记录")
        
        self.log(f"  数据处理完成，共 {len(df)} 条有效记录")
        
        return df
    
    def apply_template_with_matching(self, df: pd.DataFrame, template_columns: List[str]) -> Tuple[pd.DataFrame, Dict[str, str], List[str]]:
        """应用模板并进行智能列名匹配"""
        matching = {}
        unmatched = []
        
        for col in template_columns:
            matched_field = match_column_name(col)
            if matched_field:
                matching[col] = matched_field
            else:
                unmatched.append(col)
        
        result_df = pd.DataFrame(columns=template_columns)
        
        for col in template_columns:
            if col in matching:
                standard_field = matching[col]
                if standard_field in df.columns:
                    result_df[col] = df[standard_field]
                else:
                    result_df[col] = ''
            else:
                result_df[col] = ''
        
        return result_df, matching, unmatched
    
    def get_standard_columns(self) -> List[str]:
        """获取标准输出列名"""
        return list(self.FIELD_MAPPING.keys())


def export_data(df: pd.DataFrame, output_path: str, output_format: str):
    """根据格式导出数据"""
    df = df.fillna('')
    df = df.replace({'None': '', 'nan': '', 'NaN': ''})
    
    if output_format == 'xlsx':
        df.to_excel(output_path, index=False, engine='openpyxl')
    elif output_format == 'csv':
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
    elif output_format == 'json':
        df.to_json(output_path, orient='records', force_ascii=False, indent=2)


def process_data(inputs: List[str], year_range: Tuple[int, int], 
                 output_mode: str, template_file=None, 
                 template_columns: List[str] = None, 
                 output_format: str = 'xlsx',
                 host_class_default: str = '') -> Tuple[bool, str, List[str]]:
    """处理数据获取任务，每个物种生成单独文件"""
    global global_logs, global_progress, global_species_info, global_download_files
    
    try:
        fetcher = GBIFDataFetcher(log_callback=lambda msg: global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"))
        processor = DataProcessor(log_callback=lambda msg: global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"), 
                                  host_class_default=host_class_default)
        
        global_download_files = []
        total_inputs = len(inputs)
        
        for idx, input_str in enumerate(inputs):
            if not is_running:
                return False, "任务已取消", []
            
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] \n[{idx + 1}/{total_inputs}] 正在处理：{input_str}")
            global_progress = int((idx / total_inputs) * 100)
            
            usage_key, species_info = fetcher.resolve_input(input_str)
            
            if species_info:
                global_species_info.append(species_info)
            
            if usage_key:
                records = fetcher.fetch_occurrences(usage_key, year_range)
                
                if records:
                    df = processor.process_records(records)
                    if not df.empty:
                        if output_mode == 'template' and template_columns:
                            df, matching, unmatched = processor.apply_template_with_matching(df, template_columns)
                        else:
                            standard_cols = processor.get_standard_columns()
                            df = df[standard_cols]
                        
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        input_clean = input_str.strip()
                        if input_clean.isdigit():
                            filename = f"ID{input_clean}_点位数据_{timestamp}.{output_format}"
                        else:
                            name_part = input_clean.split()[0] if input_clean.split() else "species"
                            filename = f"{name_part}_点位数据_{timestamp}.{output_format}"
                        
                        output_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                        export_data(df, output_path, output_format)
                        global_download_files.append(filename)
                        
                        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 已保存：{filename} ({len(df)} 条记录)")
            
            time.sleep(0.5)
        
        if not global_download_files:
            return False, "未获取到任何数据", []
        
        global_progress = 100
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] \n完成！共生成 {len(global_download_files)} 个文件")
        
        return True, f"成功生成 {len(global_download_files)} 个文件", global_download_files
        
    except Exception as e:
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] \n发生错误：{str(e)}")
        return False, str(e), []


@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


@app.route('/api/parse-template', methods=['POST'])
def parse_template():
    """解析模板文件 API"""
    global global_template_info
    
    if 'template' not in request.files:
        return jsonify({'success': False, 'message': '未上传模板文件'})
    
    template_file = request.files['template']
    if template_file.filename == '':
        return jsonify({'success': False, 'message': '未选择文件'})
    
    try:
        template_path = os.path.join(app.config['UPLOAD_FOLDER'], f"template_{template_file.filename}")
        template_file.save(template_path)
        
        columns, file_format = parse_template_file(template_path)
        
        matching = {}
        unmatched = []
        for col in columns:
            matched_field = match_column_name(col)
            if matched_field:
                matching[col] = matched_field
            else:
                unmatched.append(col)
        
        global_template_info = {
            'path': template_path,
            'format': file_format,
            'columns': columns
        }
        
        return jsonify({
            'success': True,
            'format': file_format,
            'columns': columns,
            'matching': matching,
            'unmatched': unmatched
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/fetch', methods=['POST'])
def fetch():
    """获取数据 API"""
    global is_running, global_logs, global_progress, global_species_info, global_download_files
    
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
    host_class_default = request.form.get('host_class_default', '')
    
    template_columns = None
    output_format = 'xlsx'
    
    if output_mode == 'template' and global_template_info:
        template_columns = global_template_info.get('columns', [])
        output_format = global_template_info.get('format', 'xlsx')
    
    inputs = [s.strip() for s in species_input.split(',') if s.strip()]
    
    is_running = True
    global_logs = []
    global_progress = 0
    global_species_info = []
    global_download_files = []
    
    def run_task():
        global is_running
        success, message, files = process_data(
            inputs, (year_start, year_end), output_mode, 
            template_columns=template_columns, 
            output_format=output_format,
            host_class_default=host_class_default
        )
        is_running = False
    
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


@app.route('/api/download/<filename>')
def download(filename):
    """下载文件 API"""
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    return jsonify({'success': False, 'message': '文件不存在'}), 404


@app.route('/api/download-all')
def download_all():
    """一键下载所有文件 API"""
    import zipfile
    import io
    
    if not global_download_files:
        return jsonify({'success': False, 'message': '没有可下载的文件'}), 404
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"GBIF数据_{timestamp}.zip"
    
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename in global_download_files:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            if os.path.exists(file_path):
                zf.write(file_path, filename)
    
    memory_file.seek(0)
    
    return send_file(
        memory_file,
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
    app.run(debug=True, host='0.0.0.0', port=5000)
