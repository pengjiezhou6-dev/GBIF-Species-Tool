#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import re
import json
import threading
import uuid
import zipfile
import io
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pandas as pd
import pycountry
import requests
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


class GBIFAsyncDownloader:
    """
    GBIF 异步下载管理器
    使用 occurrences.download API 提交异步下载任务
    """

    def __init__(self, username: str, password: str, email: str, log_callback=None):
        self.username = username
        self.password = password
        self.email = email
        self.credentials = (username, password)
        self.log_callback = log_callback

    def log(self, message: str):
        """输出日志"""
        if self.log_callback:
            self.log_callback(message)
        else:
            print(message)

    def get_usage_keys(self, species_names: List[str]) -> List[int]:
        """
        批量解析物种名为 usageKey
        使用 species.name_backbone
        """
        usage_keys = []
        for name in species_names:
            try:
                result = species.name_backbone(name)
                if result:
                    # usageKey 在 result['usage']['key'] 中
                    usage_info = result.get('usage', {})
                    if isinstance(usage_info, dict) and 'key' in usage_info:
                        usage_keys.append(usage_info['key'])
                        self.log(f"  物种 '{name}' -> usageKey: {usage_info['key']}")
                    elif 'usageKey' in result:
                        # 兼容旧版本
                        usage_keys.append(result['usageKey'])
                        self.log(f"  物种 '{name}' -> usageKey: {result['usageKey']}")
                    elif 'key' in result:
                        # 兼容其他情况
                        usage_keys.append(result['key'])
                        self.log(f"  物种 '{name}' -> key: {result['key']}")
                    else:
                        self.log(f"  警告：无法获取 '{name}' 的 usageKey")
                else:
                    self.log(f"  警告：name_backbone 返回空结果 for '{name}'")
            except Exception as e:
                self.log(f"  错误：获取 '{name}' 的 usageKey 失败: {e}")
        self.log(f"  共解析到 {len(usage_keys)} 个 usageKey")
        return usage_keys

    def submit_download_request(
        self,
        usage_keys: List[int],
        year_range: Optional[Tuple[int, int]] = None
    ) -> str:
        """
        提交异步下载申请
        Predicates: taxonKey (IN), hasCoordinate=True, hasGeospatialIssue=False
        """
        predicates = [
            {"type": "in", "key": "TAXON_KEY", "values": usage_keys},
            {"type": "equals", "key": "HAS_COORDINATE", "value": "true"},
            {"type": "equals", "key": "HAS_GEOSPATIAL_ISSUE", "value": "false"}
        ]

        if year_range:
            predicates.append({
                "type": "equals",
                "key": "YEAR",
                "value": f"{year_range[0]},{year_range[1]}"
            })

        payload = {
            "creator": self.username,
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
            json=payload,
            auth=self.credentials
        )
        response.raise_for_status()

        download_key = response.text.strip('"')
        return download_key

    def check_download_status(self, download_key: str) -> Dict:
        """
        查询下载状态
        """
        response = requests.get(
            f"https://api.gbif.org/v1/occurrence/download/{download_key}",
            auth=self.credentials
        )
        response.raise_for_status()
        return response.json()

    def download_zip(self, download_url: str, use_auth: bool = True) -> bytes:
        """
        自动下载 ZIP 文件
        """
        if use_auth:
            response = requests.get(download_url, auth=self.credentials, stream=True, timeout=300)
        else:
            response = requests.get(download_url, stream=True, timeout=300)
        response.raise_for_status()
        return response.content


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


def process_gbif_zip_bytes(
    zip_bytes: bytes,
    output_path: str,
    host_class_default: str = '',
    log_callback=None
) -> Tuple[bool, str, int]:
    """
    处理 GBIF 返回的 ZIP 字节数据
    使用 chunksize=50000 避免内存溢出
    输出标准 11 列格式
    """
    STANDARD_COLUMNS = [
        'species', 'host_class', 'longitude', 'latitude', 'country',
        'admin1', 'year', 'source', 'n_individuals', 'obs_type', 'remarks'
    ]

    def log(msg):
        if log_callback:
            log_callback(msg)

    try:
        log("  正在解压 ZIP 文件...")
        # 从字节创建 ZipFile
        zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))

        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        if not csv_files:
            return False, "ZIP 中未找到 CSV 文件", 0

        csv_file = csv_files[0]
        log(f"  找到数据文件: {csv_file}")
        
        chunk_size = 50000
        processed_chunks = []
        total_records = 0
        chunk_count = 0

        log(f"  开始分块读取数据（每块 {chunk_size} 条）...")
        
        # 读取 CSV 并分块处理
        # GBIF SIMPLE_CSV 使用 tab 分隔符
        with zip_file.open(csv_file) as f:
            for chunk in pd.read_csv(f, chunksize=chunk_size, sep='\t', encoding='utf-8', on_bad_lines='skip'):
                chunk_count += 1
                log(f"  正在处理第 {chunk_count} 块数据...")
                
                # 字段映射和清洗（复用原有逻辑）
                result = pd.DataFrame(columns=STANDARD_COLUMNS)

                # species: 格式化为 '属名_种名'
                result['species'] = chunk.get('species', pd.Series([''] * len(chunk))).apply(format_species_code)

                # 坐标处理
                result['longitude'] = chunk.get('decimalLongitude', '')
                result['latitude'] = chunk.get('decimalLatitude', '')

                # 过滤无效坐标
                valid_mask = (
                    result['longitude'].notna() & result['latitude'].notna() &
                    (result['longitude'] != '') & (result['latitude'] != '')
                )
                result = result[valid_mask]

                if len(result) == 0:
                    log(f"  第 {chunk_count} 块无有效坐标数据，跳过")
                    continue

                # 国家代码转换
                result['country'] = chunk.get('countryCode', '').apply(convert_country_code)
                result['admin1'] = chunk.get('stateProvince', '')
                result['year'] = chunk.get('year', '')
                result['source'] = 'GBIF'
                result['obs_type'] = 'occurrence'  # 默认填入
                result['n_individuals'] = chunk.get('individualCount', 1)

                # host_class: 使用默认值或根据记录判断
                result['host_class'] = host_class_default
                result['remarks'] = ''

                processed_chunks.append(result)
                total_records += len(result)

                log(f"  第 {chunk_count} 块处理完成，累计 {total_records} 条记录")

        if not processed_chunks:
            return False, "没有有效记录", 0

        log(f"  数据读取完成，共 {total_records} 条记录，开始合并...")
        
        # 合并所有块
        final_df = pd.concat(processed_chunks, ignore_index=True)

        log(f"  数据合并完成，开始去重...")
        
        # 去重
        original_count = len(final_df)
        final_df = final_df.drop_duplicates(subset=['species', 'longitude', 'latitude', 'year'], keep='first')
        dedup_count = len(final_df)

        if original_count > dedup_count:
            log(f"  自动去重：移除了 {original_count - dedup_count} 条重复记录")

        log(f"  开始导出文件: {output_path}")
        
        # 导出
        if output_path.endswith('.xlsx'):
            final_df.to_excel(output_path, index=False, engine='openpyxl')
        else:
            final_df.to_csv(output_path, index=False, encoding='utf-8-sig')

        log(f"  文件导出完成")

        return True, f"成功处理 {len(final_df)} 条记录", len(final_df)

    except Exception as e:
        log(f"  处理出错: {str(e)}")
        import traceback
        log(f"  错误详情: {traceback.format_exc()}")
        return False, f"处理失败: {str(e)}", 0


def process_data(
    inputs: List[str],
    year_range: Tuple[int, int],
    output_mode: str,
    template_file=None,
    template_columns: List[str] = None,
    output_format: str = 'xlsx',
    host_class_default: str = '',
    gbif_credentials: Dict = None
) -> Tuple[bool, str, List[str]]:
    """
    处理数据获取任务 - 使用异步下载方式
    自动下载 ZIP 并清洗
    """
    global global_logs, global_progress, global_species_info, global_download_files

    if not gbif_credentials:
        return False, "请提供 GBIF 账号信息", []
    
    try:
        # 创建下载器
        downloader = GBIFAsyncDownloader(
            gbif_credentials['username'],
            gbif_credentials['password'],
            gbif_credentials['email'],
            log_callback=lambda msg: global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        )

        global_download_files = []

        # 1. 解析所有物种为 usageKeys
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 正在解析 {len(inputs)} 个物种...")
        usage_keys = downloader.get_usage_keys(inputs)

        if not usage_keys:
            return False, "未找到有效的物种 usageKey", []

        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 成功获取 {len(usage_keys)} 个 usageKeys")

        # 2. 检查是否有可复用的历史下载
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
            # 3. 提交新的下载申请
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 未找到匹配的历史下载，提交新申请...")
            download_key = downloader.submit_download_request(usage_keys, year_range)
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 下载申请已提交，Download Key: {download_key}")
            global_progress = 10

            # 4. 自动轮询状态（仅对新申请）
            global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 等待 GBIF 处理（这可能需要几分钟到几小时）...")

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

        # 5. 自动下载 ZIP
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] 正在下载数据...")
        
        if not download_url:
            return False, "无法获取下载链接", []

        zip_bytes = downloader.download_zip(download_url)
        global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ZIP 下载完成，大小: {len(zip_bytes) / 1024 / 1024:.2f} MB")
        global_progress = 60

        # 5. 自动处理 ZIP
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

        # 6. 应用模板（如果需要）
        if output_mode == 'template' and template_columns:
            # 读取处理后的文件
            if output_format == 'xlsx':
                df = pd.read_excel(output_path)
            else:
                df = pd.read_csv(output_path)

            # 使用 DataProcessor 应用模板
            processor = DataProcessor(
                log_callback=lambda msg: global_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"),
                host_class_default=host_class_default
            )
            df, matching, unmatched = processor.apply_template_with_matching(df, template_columns)

            # 重新保存
            export_data(df, output_path, output_format)
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
    """获取数据 API - 使用异步下载方式"""
    global is_running, global_logs, global_progress, global_species_info, global_download_files

    if is_running:
        return jsonify({'success': False, 'message': '已有任务正在运行'})

    species_input = request.form.get('species_input', '').strip()
    if not species_input:
        return jsonify({'success': False, 'message': '请输入物种学名或 ID'})

    # 获取 GBIF 凭据
    gbif_username = request.form.get('gbif_username', '').strip()
    gbif_password = request.form.get('gbif_password', '').strip()
    gbif_email = request.form.get('gbif_email', '').strip()

    if not all([gbif_username, gbif_password, gbif_email]):
        return jsonify({'success': False, 'message': '请填写完整的 GBIF 账号信息（Username, Password, Email）'})

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
                'key': item.get('key'),
                'status': item.get('status'),
                'created': item.get('created'),
                'modified': item.get('modified'),
                'downloadLink': item.get('downloadLink'),
                'size': item.get('size'),
                'totalRecords': item.get('totalRecords'),
                'predicate': item.get('request', {}).get('predicate', {})
            })
        
        return jsonify({
            'success': True,
            'downloads': downloads
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


def find_matching_download(username: str, password: str, usage_keys: List[int], year_range: Optional[Tuple[int, int]] = None) -> Optional[Dict]:
    """查找是否有匹配的历史下载"""
    try:
        result = occ.download_list(user=username, pwd=password, limit=50)
        
        for item in result.get('results', []):
            if item.get('status') != 'SUCCEEDED':
                continue
            
            predicate = item.get('request', {}).get('predicate', {})
            if not predicate:
                continue
            
            historical_keys = set()
            if predicate.get('type') == 'and':
                for p in predicate.get('predicates', []):
                    if p.get('key') == 'TAXON_KEY':
                        if p.get('type') == 'in':
                            values = p.get('values', [])
                            historical_keys = set(str(v) for v in values)
                        elif p.get('type') == 'equals':
                            historical_keys = {str(p.get('value'))}
            
            current_keys = set(str(k) for k in usage_keys)
            if historical_keys == current_keys:
                return item
        
        return None
        
    except Exception as e:
        print(f"查找历史下载失败: {e}")
        return None


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
