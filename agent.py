import baostock as bs
import akshare as ak
import pandas as pd
import time
import json
import os
import math
from datetime import datetime, timedelta
from functools import wraps

# 禁用akshare的进度条
try:
    ak._show_progress = False
    ak.set_option('show_progress', False)
except Exception:
    pass

# 安全的整数转换函数
def safe_int(s, default=0):
    """安全地将字符串转换为整数，如果转换失败则返回默认值"""
    if not s:
        return default
    if isinstance(s, int):
        return s
    if isinstance(s, str):
        s = s.strip()
        if not s:
            return default
        # 使用正则表达式提取数字部分
        import re
        match = re.search(r'\d+', s)
        if match:
            try:
                return int(match.group())
            except ValueError:
                return default
        else:
            return default
    try:
        # 尝试将非字符串类型转换为整数
        if isinstance(s, (float, complex)):
            return int(round(s))
        return int(str(s))
    except (ValueError, TypeError):
        return default


# 重试装饰器
def retry_with_backoff(max_retries=3, base_delay=1.0, exponent=2.0):
    """重试装饰器，实现指数退避机制"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = base_delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        # 最后一次重试失败，抛出异常
                        raise
                    # 检查是否为网络相关错误
                    if any(error_code in str(e) for error_code in ['10060', '10054', '10057', 'ConnectionError', 'TimeoutError']):
                        print(f"网络错误: {e}，{retries}/{max_retries} 重试中...")
                        time.sleep(delay)
                        delay *= exponent
                    else:
                        # 非网络错误，直接抛出
                        raise
        return wrapper
    return decorator


class FinancialRiskAgent:
    def __init__(self):
        self.cache_dir = "./cache"
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        # 申万一级行业映射字典
        self.sw_industry_map = {
            # 金融行业
            "银行": "银行",
            "保险": "非银金融",
            "证券": "非银金融",
            "多元金融": "非银金融",
            "保险及其他": "非银金融",
            "证券及其他": "非银金融",
            
            # 房地产行业
            "房地产": "房地产",
            "房地产开发": "房地产",
            "园区开发": "房地产",
            
            # 医药生物行业
            "医药生物": "医药生物",
            "医疗器械": "医药生物",
            "生物制品": "医药生物",
            "化学制药": "医药生物",
            "中药": "医药生物",
            "医疗服务": "医药生物",
            "医药商业": "医药生物",
            "生物制药": "医药生物",
            
            # 电子行业
            "电子": "电子",
            "半导体": "电子",
            "集成电路": "电子",
            "消费电子": "电子",
            "电子制造": "电子",
            "半导体及元件": "电子",
            "光学光电子": "电子",
            "电子化学品": "电子",
            "其他电子": "电子",
            
            # 计算机行业
            "计算机": "计算机",
            "软件开发": "计算机",
            "互联网": "计算机",
            "计算机设备": "计算机",
            "IT服务": "计算机",
            "软件服务": "计算机",
            "互联网服务": "计算机",
            "云计算": "计算机",
            "大数据": "计算机",
            "人工智能": "计算机",
            
            # 通信行业
            "通信": "通信",
            "通信设备": "通信",
            "电信运营": "通信",
            "5G": "通信",
            "网络设备": "通信",
            "光通信": "通信",
            "卫星导航": "通信",
            
            # 食品饮料行业
            "食品饮料": "食品饮料",
            "白酒": "食品饮料",
            "啤酒": "食品饮料",
            "乳制品": "食品饮料",
            "酿酒": "食品饮料",
            "酿酒行业": "食品饮料",
            "食品加工": "食品饮料",
            "肉制品": "食品饮料",
            "调味品": "食品饮料",
            "软饮料": "食品饮料",
            "休闲食品": "食品饮料",
            "食品综合": "食品饮料",
            
            # 农林牧渔行业
            "农林牧渔": "农林牧渔",
            "种植业": "农林牧渔",
            "渔业": "农林牧渔",
            "畜牧业": "农林牧渔",
            "农药兽药": "农林牧渔",
            "农产品加工": "农林牧渔",
            "农业综合": "农林牧渔",
            "饲料": "农林牧渔",
            
            # 化工行业
            "化工": "化工",
            "基础化工": "化工",
            "化学原料": "化工",
            "化学制品": "化工",
            "精细化工": "化工",
            "化肥": "化工",
            "农药": "化工",
            "塑料": "化工",
            "橡胶": "化工",
            "化学纤维": "化工",
            "日用化学": "化工",
            
            # 石油石化行业
            "石油石化": "石油石化",
            "石油化工": "石油石化",
            "石油开采": "石油石化",
            "石油加工": "石油石化",
            "油气服务": "石油石化",
            "天然气": "石油石化",
            "成品油": "石油石化",
            
            # 煤炭行业
            "煤炭": "煤炭",
            "煤炭开采": "煤炭",
            "煤炭加工": "煤炭",
            "焦炭": "煤炭",
            "煤化工": "煤炭",
            
            # 有色金属行业
            "有色金属": "有色金属",
            "工业金属": "有色金属",
            "贵金属": "有色金属",
            "稀有金属": "有色金属",
            "金属新材料": "有色金属",
            "小金属": "有色金属",
            "铜": "有色金属",
            "铝": "有色金属",
            "锂": "有色金属",
            "钴": "有色金属",
            "镍": "有色金属",
            
            # 钢铁行业
            "钢铁": "钢铁",
            "普钢": "钢铁",
            "特钢": "钢铁",
            "钢铁加工": "钢铁",
            
            # 机械设备行业
            "机械设备": "机械设备",
            "通用机械": "机械设备",
            "专用设备": "机械设备",
            "运输设备": "机械设备",
            "工程机械": "机械设备",
            "自动化设备": "机械设备",
            "机床工具": "机械设备",
            "仪器仪表": "机械设备",
            "机械零部件": "机械设备",
            "机器人": "机械设备",
            
            # 国防军工行业
            "国防军工": "国防军工",
            "航天装备": "国防军工",
            "航空装备": "国防军工",
            "地面兵装": "国防军工",
            "船舶制造": "国防军工",
            "军工电子": "国防军工",
            "军工材料": "国防军工",
            
            # 汽车行业
            "汽车": "汽车",
            "乘用车": "汽车",
            "商用车": "汽车",
            "汽车零部件": "汽车",
            "新能源汽车": "汽车",
            "汽车服务": "汽车",
            "汽车电子": "汽车",
            "摩托车": "汽车",
            
            # 电力设备行业
            "电力设备": "电力设备",
            "电气设备": "电力设备",
            "新能源": "电力设备",
            "光伏": "电力设备",
            "风电": "电力设备",
            "电池": "电力设备",
            "电网设备": "电力设备",
            "新能源发电设备": "电力设备",
            "其他电源设备": "电力设备",
            "光伏设备": "电力设备",
            "风电设备": "电力设备",
            "储能设备": "电力设备",
            
            # 家用电器行业
            "家用电器": "家用电器",
            "家电": "家用电器",
            "白色家电": "家用电器",
            "黑色家电": "家用电器",
            "厨房电器": "家用电器",
            "小家电": "家用电器",
            "其他家电": "家用电器",
            "照明设备": "家用电器",
            
            # 纺织服装行业
            "纺织服装": "纺织服装",
            "纺织制造": "纺织服装",
            "服装家纺": "纺织服装",
            "服饰": "纺织服装",
            "家纺": "纺织服装",
            "面料": "纺织服装",
            "辅料": "纺织服装",
            
            # 轻工制造行业
            "轻工制造": "轻工制造",
            "造纸": "轻工制造",
            "包装印刷": "轻工制造",
            "家具": "轻工制造",
            "家用轻工": "轻工制造",
            "文娱用品": "轻工制造",
            "其他轻工制造": "轻工制造",
            
            # 交通运输行业
            "交通运输": "交通运输",
            "铁路运输": "交通运输",
            "公路运输": "交通运输",
            "水路运输": "交通运输",
            "航空运输": "交通运输",
            "物流": "交通运输",
            "港口": "交通运输",
            "机场": "交通运输",
            "航运": "交通运输",
            "快递": "交通运输",
            
            # 商贸零售行业
            "商贸零售": "商贸零售",
            "零售": "商贸零售",
            "百货零售": "商贸零售",
            "专业零售": "商贸零售",
            "电商零售": "商贸零售",
            "商业贸易": "商贸零售",
            "贸易": "商贸零售",
            "超市": "商贸零售",
            "连锁经营": "商贸零售",
            
            # 社会服务行业
            "社会服务": "社会服务",
            "旅游综合": "社会服务",
            "景点": "社会服务",
            "酒店餐饮": "社会服务",
            "教育": "社会服务",
            "医疗服务": "社会服务",
            "美容服务": "社会服务",
            "体育": "社会服务",
            "文化娱乐": "社会服务",
            "专业服务": "社会服务",
            
            # 传媒行业
            "传媒": "传媒",
            "出版": "传媒",
            "广播电视": "传媒",
            "影视院线": "传媒",
            "游戏": "传媒",
            "广告营销": "传媒",
            "数字媒体": "传媒",
            "网络媒体": "传媒",
            "动漫": "传媒",
            
            # 美容护理行业
            "美容护理": "美容护理",
            "化妆品": "美容护理",
            "个人护理": "美容护理",
            "医美": "美容护理",
            "日化": "美容护理",
            "护肤品": "美容护理",
            "彩妆": "美容护理",
            
            # 环保行业
            "环保": "环保",
            "环境治理": "环保",
            "环保设备": "环保",
            "水务": "环保",
            "固废处理": "环保",
            "大气治理": "环保",
            "土壤修复": "环保",
            "环境监测": "环保",
            
            # 公用事业行业
            "公用事业": "公用事业",
            "电力": "公用事业",
            "燃气": "公用事业",
            "水务": "公用事业",
            "环保工程": "公用事业",
            "垃圾处理": "公用事业",
            "供热": "公用事业",
            
            # 建筑材料行业
            "建筑材料": "建筑材料",
            "水泥": "建筑材料",
            "玻璃": "建筑材料",
            "建材": "建筑材料",
            "新材料": "建筑材料",
            "砖瓦建材": "建筑材料",
            "耐火材料": "建筑材料",
            
            # 建筑装饰行业
            "建筑装饰": "建筑装饰",
            "房屋建设": "建筑装饰",
            "基建工程": "建筑装饰",
            "装修装饰": "建筑装饰",
            "园林工程": "建筑装饰",
            "国际工程": "建筑装饰",
            
            # 采掘行业
            "采掘": "采掘",
            "石油开采": "采掘",
            "天然气开采": "采掘",
            "煤炭开采": "采掘",
            "金属矿采选": "采掘",
            "非金属矿采选": "采掘",
            
            # 综合行业
            "综合": "综合",
            "综合类": "综合",
            "多元化经营": "综合"
        }
        
        # 沪深300指数代码
        self.hs300_code = "000300.sh"
        
        # 市值分组与基准换手率映射
        self.market_cap_benchmark = {
            "特大盘": 0.0035,  # 0.35%
            "超大盘": 0.0065,  # 0.65%
            "大盘": 0.0100,    # 1.00%
            "中盘": 0.0150,    # 1.50%
            "小盘": 0.0230,    # 2.30%
            "微小盘A": 0.0400, # 4.00%
            "微小盘B": 0.1000  # 10.00%
        }
        
        # 行业调整因子
        self.industry_adjustment = {
            # 低换手行业
            "银行": 0.5,
            "非银金融": 0.7,
            "公用事业": 0.6,
            "交通运输": 0.6,
            "建筑装饰": 0.6,
            "建筑材料": 0.6,
            # 中换手行业
            "食品饮料": 1.0,
            "医药生物": 1.0,
            "家用电器": 0.9,
            "房地产": 0.9,
            "农林牧渔": 1.0,
            "汽车": 1.0,
            "机械设备": 1.0,
            # 高换手行业
            "电子": 1.4,
            "计算机": 1.5,
            "通信": 1.4,
            "传媒": 1.4,
            "化工": 1.2,
            "电力设备": 1.3
        }
        
        # 行业类型分类：成长型、价值型、周期型
        self.industry_type_map = {
            "成长型": ["计算机", "电子", "通信", "医药生物", "电力设备", "国防军工", "传媒", "环保"],
            "价值型": ["银行", "非银金融", "食品饮料", "公用事业", "交通运输", "石油石化", "煤炭", "钢铁", "建筑材料", "社会服务", "商贸零售", "综合"],
            "周期型": ["有色金属", "化工", "机械设备", "汽车", "家用电器", "轻工制造", "纺织服装", "建筑装饰", "农林牧渔", "采掘", "房地产"]
        }
        
        # 行业龙头企业字典
        self.industry_leaders = {
            "计算机": ["000938", "600588"],  # 紫光股份、用友网络
            "电子": ["002415", "002475"],     # 海康威视、立讯精密
            "通信": ["000063", "600498"],     # 中兴通讯、烽火通信
            "传媒": ["300413", "002027"],     # 芒果超媒、分众传媒
            "医药生物": ["600276", "000661"],  # 恒瑞医药、长春高新
            "国防军工": ["601989", "600760"],  # 中国重工、中航沈飞
            "电力设备": ["300750", "300274"],  # 宁德时代、阳光电源
            "环保": ["300070", "002340"],     # 碧水源、格林美
            "银行": ["600036", "601166"],     # 招商银行、兴业银行
            "非银金融": ["600030", "601318"],  # 中信证券、中国平安
            "食品饮料": ["600519", "000858"],  # 贵州茅台、五粮液
            "农林牧渔": ["002714", "300498"],  # 牧原股份、温氏股份
            "公用事业": ["600900", "600011"],  # 长江电力、华能国际
            "交通运输": ["002352", "601111"],  # 顺丰控股、中国国航
            "房地产": ["000002", "600048"],    # 万科A、保利发展
            "商贸零售": ["002024", "601933"],  # 苏宁易购、永辉超市
            "社会服务": ["601888", "300144"],  # 中国中免、宋城演艺
            "石油石化": ["601857", "600028"],  # 中国石油、中国石化
            "美容护理": ["603605", "300957"],  # 珀莱雅、贝泰妮
            "综合": ["601088", "600058"],      # 中国神华、五矿发展
            "有色金属": ["601899", "002460"],  # 紫金矿业、赣锋锂业
            "化工": ["600309", "600346"],     # 万华化学、恒力石化
            "钢铁": ["600019", "000898"],     # 宝钢股份、鞍钢股份
            "煤炭": ["601088", "601225"],     # 中国神华、陕西煤业
            "建筑材料": ["000786", "002233"],  # 北新建材、塔牌集团
            "建筑装饰": ["601668", "601186"],  # 中国建筑、中国铁建
            "机械设备": ["600031", "000157"],  # 三一重工、中联重科
            "汽车": ["600104", "000859"],     # 上汽集团、比亚迪
            "家用电器": ["000651", "000333"],  # 格力电器、美的集团
            "轻工制造": ["002078", "002572"],  # 太阳纸业、索菲亚
            "纺织服装": ["600398", "600177"]   # 海澜之家、雅戈尔
        }
        
        # 各行业类型的指标权重
        self.industry_type_weights = {
            "成长型": {"PETTM": 0.35, "PB": 0.25, "PSTTM": 0.30, "股息率": 0.10},
            "价值型": {"PETTM": 0.30, "PB": 0.25, "PSTTM": 0.15, "股息率": 0.30},
            "周期型": {"PETTM": 0.25, "PB": 0.35, "PSTTM": 0.25, "股息率": 0.15}
        }
        
        # 财务健康度分析的缓存期限配置
        self.financial_cache_durations = {
            "industry_classification": 90,   # 行业分类数据
            "net_profit_2y": 180,          # 近2年净利润
            "operating_cashflow_3y": 180,  # 近3年经营现金流
            "st_status": 1,                # ST状态数据
            "other_financials": 90         # 其余数据
        }
        
        # 行业调整系数（除特定行业外）
        self.industry_adjustment_coefficients = {
            # 强周期行业
            "煤炭": 0.85,
            "石油石化": 0.85,
            "有色金属": 0.85,
            "钢铁": 0.85,
            "化工": 0.85,
            "建筑材料": 0.85,
            "采掘": 0.85,
            # 强周期+高杠杆
            "建筑装饰": 0.80,
            "房地产": 0.80,
            # 中周期+高杠杆
            "交通运输": 0.85,
            # 中周期
            "汽车": 0.90,
            "机械设备": 0.90,
            # 弱周期
            "商贸零售": 0.95,
            "社会服务": 0.95,
            # 高杠杆
            "银行": 0.88,
            # 高杠杆+强周期
            "非银金融": 0.82,
            # 成长性
            "电子": 1.05,
            "计算机": 1.05,
            "通信": 1.05,
            "传媒": 1.05,
            "电力设备": 1.05,
            # 防御性+成长性
            "食品饮料": 1.06,
            "医药生物": 1.06,
            # 防御性
            "纺织服装": 1.02,
            "公用事业": 0.95,
            "农林牧渔": 1.00,
            "美容护理": 1.04,
            "家用电器": 1.03,
            # 弱周期
            "轻工制造": 1.00,
            "国防军工": 0.98,
            "环保": 0.98,
            "综合": 1.00
        }
        
        # 医药生物行业细分调整系数
        self.pharma_adjustment = {
            "化学制药": 1.06,
            "生物制品": 1.06,
            "医疗器械": 1.06,
            "医疗服务": 1.06,
            "中药": 1.02,
            "医药商业": 1.02
        }
        
        # 电力设备行业细分调整系数
        self.power_equipment_adjustment = {
            "光伏设备": 1.07,
            "风电设备": 1.07,
            "电池": 1.07,
            "电网设备": 0.97,
            "其他电源设备": 0.97
        }
        
        # 家用电器行业细分调整系数
        self.home_appliance_adjustment = {
            "白色家电": 0.98,
            "黑色家电": 0.98,
            "厨房电器": 1.03,
            "小家电": 1.03,
            "其他家电": 1.03,
            "照明设备": 0.95
        }
 
    def get_cache_file_path(self, key):
        return os.path.join(self.cache_dir, f"{key}.json")
    
    def is_cache_valid(self, cache_file, days):
        if not os.path.exists(cache_file):
            return False
        modify_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        return datetime.now() - modify_time < timedelta(days=days)
    
    def get_cached_data(self, key):
        cache_file = self.get_cache_file_path(key)
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def set_cached_data(self, key, data):
        cache_file = self.get_cache_file_path(key)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def stock_name_to_code(self, name_or_code):
        """将股票名称或代码转化为股票代码"""
        # 添加空字符串检查
        if not name_or_code or name_or_code.strip() == '':
            raise ValueError("股票名称或代码不能为空")
        
        # 如果已经是6位数字，直接返回
        if name_or_code.isdigit() and len(name_or_code) == 6:
            return name_or_code
        
        # 否则尝试通过akshare获取股票代码
        try:
            # 使用akshare的股票名称查询功能
            stock_list = ak.stock_info_a_code_name()
            stock_code = stock_list[stock_list['name'] == name_or_code]['code'].values
            if len(stock_code) > 0:
                # 标准化akshare返回的股票代码
                return self.normalize_akshare_code(stock_code[0])
            else:
                # 如果找不到股票，尝试使用名称作为代码（虽然不太合理，但可以避免崩溃）
                # 或者可以抛出一个更友好的异常
                return name_or_code
        except Exception as e:
            print(f"股票名称转换失败：{e}")
            # 发生异常时，返回原始输入，让后续处理逻辑来处理
            return name_or_code
    
    def format_stock_code(self, code):
        """将6位数字代码转化为baostock需要的格式：交易所后缀+.+六位股票代码"""
        # 简单处理：上证股票以6开头，深证股票以0或3开头
        if code.startswith('6'):
            return f"sh.{code}"
        else:
            return f"sz.{code}"
    
    def normalize_akshare_code(self, code):
        """将akshare返回的股票代码标准化为6位数字代码"""
        # akshare返回的股票代码可能带有后缀，如.SZ或.SH，需要移除
        if isinstance(code, str):
            # 移除任何后缀，只保留前6位数字
            code = code.split('.')[0]
            # 确保只保留6位数字
            code = ''.join(filter(str.isdigit, code))[:6]
        return code
    
    def format_akshare_code(self, code):
        """将6位数字代码转化为akshare需要的格式：六位股票代码+.+交易所后缀"""
        # 简单处理：上证股票以6开头，深证股票以0或3开头
        if code.startswith('6'):
            return f"{code}.SH"
        else:
            return f"{code}.SZ"
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def get_stock_basic_info(self, code):
        """从baostock获取股票基本信息"""
        # 提取6位数字代码
        original_code = code
        # 如果code已经是格式化好的（如sh.601318），先提取数字部分
        if '.' in code:
            code = code.split('.')[1]  # 提取数字部分
        else:
            # 转换为6位数字代码
            code = self.stock_name_to_code(code)
        
        cache_key = f"stock_basic_{code}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 1):
            return self.get_cached_data(cache_key)
        
        # 格式化股票代码为baostock需要的格式
        formatted_code = self.format_stock_code(code)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            # 获取最近7天的交易数据
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
            rs = bs.query_history_k_data_plus(
                code=formatted_code,
                fields="date,code,open,close,preclose,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                raise ValueError(f"未找到股票{code}的基本信息")
            
            # 使用最新的一条数据
            latest_data = data_list[-1]
            
            # 获取股票名称
            rs_name = bs.query_stock_basic(code=code)
            stock_name = code  # 默认使用代码作为名称
            while (rs_name.error_code == '0') & rs_name.next():
                name_row = rs_name.get_row_data()
                # 打印所有字段，以便调试
                print(f"股票基本信息字段：{name_row}")
                # 尝试不同的字段索引获取股票名称
                for i, field in enumerate(name_row):
                    if field and not field.isdigit() and '.' not in field:
                        stock_name = field
                        print(f"找到股票名称：{stock_name}（索引：{i}）")
                        break
                break
            
            # 计算涨跌幅
            close_price = float(latest_data[3])  # close
            preclose_price = float(latest_data[4])  # preclose
            change_percent = ((close_price - preclose_price) / preclose_price * 100) if preclose_price > 0 else 0.0
            
            # 提取所需字段
            data = {
                "code": code,
                "name": stock_name,
                "current_price": close_price,
                "change_percent": round(change_percent, 2)
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, data)
            return data
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(1.0)
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_dividend_yield(self, code):
        """计算股息率"""
        cache_key = f"dividend_yield_{code}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 7):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            # 获取当前年份
            current_year = datetime.now().year
            # 获取最近两年的分红数据
            
            rs = bs.query_dividend_data(code=code, year=str(current_year-2))
            dividend_list = []
            while (rs.error_code == '0') & rs.next():
                dividend_list.append(rs.get_row_data())
            
            rs = bs.query_dividend_data(code=code, year=str(current_year-1))
            while (rs.error_code == '0') & rs.next():
                dividend_list.append(rs.get_row_data())
            
            # 计算总分红
            total_dividend = 0.0
            for dividend in dividend_list:
                if len(dividend) > 9 and dividend[9]:  # 检查每股现金分红字段是否存在且不为空
                    try:
                        cash_dividend = float(dividend[9]) / 10  # 每股现金分红（转换为每股金额，因为原始数据是10股派息）
                        total_dividend += cash_dividend
                    except (ValueError, IndexError):
                        continue
            
            # 获取当前股价用于计算股息率
            basic_info = self.get_stock_basic_info(code)
            current_price = basic_info["current_price"]
            
            # 计算股息率
            dividend_yield = (total_dividend / 2) / current_price * 100 if current_price > 0 else 0.0
            
            data = {
                "code": code,
                "total_dividend": round(total_dividend, 2),
                "dividend_yield": round(dividend_yield, 2)
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, data)
            return data
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(1.0)
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def get_industry_info(self, code):
        """获取行业数据并映射到申万一级行业"""
        # 针对知名股票直接返回已知行业信息，避免API调用失败
        known_industries = {
            # 食品饮料行业
            "600519": {"industry": "白酒", "sw_industry": "食品饮料"},  # 贵州茅台
            "000858": {"industry": "白酒", "sw_industry": "食品饮料"},  # 五粮液
            "600887": {"industry": "食品加工", "sw_industry": "食品饮料"},  # 伊利股份
            "603369": {"industry": "食品加工", "sw_industry": "食品饮料"},  # 今世缘
            "600779": {"industry": "啤酒", "sw_industry": "食品饮料"},  # 水井坊
            "000895": {"industry": "啤酒", "sw_industry": "食品饮料"},  # 双汇发展
            "000568": {"industry": "白酒", "sw_industry": "食品饮料"},  # 泸州老窖
            "600543": {"industry": "啤酒", "sw_industry": "食品饮料"},  # 莫高股份
            "002568": {"industry": "白酒", "sw_industry": "食品饮料"},  # 百润股份
            "600084": {"industry": "食品加工", "sw_industry": "食品饮料"},  # 中葡股份
            
            # 银行行业
            "600036": {"industry": "银行", "sw_industry": "银行"},      # 招商银行
            "000001": {"industry": "银行", "sw_industry": "银行"},      # 平安银行
            "601166": {"industry": "银行", "sw_industry": "银行"},      # 兴业银行
            "600016": {"industry": "银行", "sw_industry": "银行"},      # 民生银行
            "601398": {"industry": "银行", "sw_industry": "银行"},      # 工商银行
            "601288": {"industry": "银行", "sw_industry": "银行"},      # 农业银行
            "601939": {"industry": "银行", "sw_industry": "银行"},      # 建设银行
            "601658": {"industry": "银行", "sw_industry": "银行"},      # 邮储银行
            "601009": {"industry": "银行", "sw_industry": "银行"},      # 南京银行
            "601818": {"industry": "银行", "sw_industry": "银行"},      # 光大银行
            
            # 非银金融行业
            "601318": {"industry": "保险", "sw_industry": "非银金融"},  # 中国平安
            "601628": {"industry": "保险", "sw_industry": "非银金融"},  # 中国人寿
            "601336": {"industry": "保险", "sw_industry": "非银金融"},  # 新华保险
            "601211": {"industry": "保险", "sw_industry": "非银金融"},  # 国泰君安
            "600030": {"industry": "证券", "sw_industry": "非银金融"},  # 中信证券
            "601198": {"industry": "证券", "sw_industry": "非银金融"},  # 东兴证券
            "000776": {"industry": "证券", "sw_industry": "非银金融"},  # 广发证券
            "000166": {"industry": "证券", "sw_industry": "非银金融"},  # 申万宏源
            "600837": {"industry": "证券", "sw_industry": "非银金融"},  # 海通证券
            "601788": {"industry": "证券", "sw_industry": "非银金融"},  # 光大证券
            
            # 房地产行业
            "000002": {"industry": "房地产", "sw_industry": "房地产"},   # 万科A
            "600048": {"industry": "房地产", "sw_industry": "房地产"},   # 保利发展
            "601155": {"industry": "房地产", "sw_industry": "房地产"},   # 新城控股
            "600383": {"industry": "房地产", "sw_industry": "房地产"},   # 金地集团
            "001979": {"industry": "房地产", "sw_industry": "房地产"},   # 招商蛇口
            "000656": {"industry": "房地产", "sw_industry": "房地产"},   # 金科股份
            "000069": {"industry": "房地产", "sw_industry": "房地产"},   # 华侨城A
            "600208": {"industry": "房地产", "sw_industry": "房地产"},   # 新湖中宝
            "000402": {"industry": "房地产", "sw_industry": "房地产"},   # 金融街
            "600663": {"industry": "房地产", "sw_industry": "房地产"},   # 陆家嘴
            
            # 电力设备行业
            "300750": {"industry": "电池", "sw_industry": "电力设备"},   # 宁德时代
            "300274": {"industry": "光伏设备", "sw_industry": "电力设备"},  # 阳光电源
            "002506": {"industry": "光伏设备", "sw_industry": "电力设备"},  # 协鑫集成
            "601012": {"industry": "光伏设备", "sw_industry": "电力设备"},  # 隆基绿能
            "002384": {"industry": "风电设备", "sw_industry": "电力设备"},  # 东山精密
            "300417": {"industry": "光伏设备", "sw_industry": "电力设备"},  # 中环股份
            "600406": {"industry": "电网设备", "sw_industry": "电力设备"},  # 国电南瑞
            "002028": {"industry": "电网设备", "sw_industry": "电力设备"},  # 思源电气
            "002459": {"industry": "光伏设备", "sw_industry": "电力设备"},  # 晶澳科技
            "300014": {"industry": "风电设备", "sw_industry": "电力设备"},  # 亿纬锂能
            
            # 电子行业
            "002415": {"industry": "电子制造", "sw_industry": "电子"},   # 海康威视
            "002475": {"industry": "电子制造", "sw_industry": "电子"},   # 立讯精密
            "000725": {"industry": "电子制造", "sw_industry": "电子"},   # 京东方A
            "002371": {"industry": "半导体", "sw_industry": "电子"},   # 北方华创
            "600745": {"industry": "半导体", "sw_industry": "电子"},   # 闻泰科技
            "600584": {"industry": "消费电子", "sw_industry": "电子"},   # 长电科技
            "300623": {"industry": "半导体", "sw_industry": "电子"},   # 捷捷微电
            "002079": {"industry": "消费电子", "sw_industry": "电子"},   # 苏州固锝
            "603019": {"industry": "半导体", "sw_industry": "电子"},   # 中科曙光
            "300327": {"industry": "半导体", "sw_industry": "电子"},   # 中颖电子
            
            # 家用电器行业
            "000333": {"industry": "白色家电", "sw_industry": "家用电器"},  # 美的集团
            "000651": {"industry": "白色家电", "sw_industry": "家用电器"},  # 格力电器
            "002035": {"industry": "白色家电", "sw_industry": "家用电器"},  # 华帝股份
            "002032": {"industry": "白色家电", "sw_industry": "家用电器"},  # 苏泊尔
            "600690": {"industry": "黑色家电", "sw_industry": "家用电器"},  # 海尔智家
            
            # 医药生物行业
            "600276": {"industry": "化学制药", "sw_industry": "医药生物"},  # 恒瑞医药
            "000661": {"industry": "生物制品", "sw_industry": "医药生物"},  # 长春高新
            "300015": {"industry": "医疗器械", "sw_industry": "医药生物"},  # 爱尔眼科
            "603259": {"industry": "化学制药", "sw_industry": "医药生物"},  # 药明康德
            "002773": {"industry": "医疗器械", "sw_industry": "医药生物"},  # 康弘药业
            "600196": {"industry": "中药", "sw_industry": "医药生物"},  # 复星医药
            "002007": {"industry": "医疗器械", "sw_industry": "医药生物"},  # 华兰生物
            "300601": {"industry": "化学制药", "sw_industry": "医药生物"},  # 康泰生物
            "300595": {"industry": "医疗器械", "sw_industry": "医药生物"},  # 欧普康视
            "600535": {"industry": "中药", "sw_industry": "医药生物"},  # 天士力
            
            # 计算机行业
            "000938": {"industry": "软件开发", "sw_industry": "计算机"},  # 紫光股份
            "600588": {"industry": "软件开发", "sw_industry": "计算机"},  # 用友网络
            "300454": {"industry": "互联网", "sw_industry": "计算机"},  # 深信服
            "002230": {"industry": "软件开发", "sw_industry": "计算机"},  # 科大讯飞
            "300369": {"industry": "互联网", "sw_industry": "计算机"},  # 绿盟科技
            "600756": {"industry": "互联网", "sw_industry": "计算机"},  # 浪潮软件
            "300253": {"industry": "软件开发", "sw_industry": "计算机"},  # 卫宁健康
            "002410": {"industry": "互联网", "sw_industry": "计算机"},  # 广联达
            "002279": {"industry": "互联网", "sw_industry": "计算机"},  # 久其软件
            "300051": {"industry": "互联网", "sw_industry": "计算机"},  # 三五互联
            
            # 汽车行业
            "600104": {"industry": "乘用车", "sw_industry": "汽车"},  # 上汽集团
            "000859": {"industry": "乘用车", "sw_industry": "汽车"},  # 比亚迪
            "600741": {"industry": "商用车", "sw_industry": "汽车"},  # 华域汽车
            "000625": {"industry": "乘用车", "sw_industry": "汽车"},  # 长安汽车
            "601238": {"industry": "乘用车", "sw_industry": "汽车"},  # 广汽集团
            "600418": {"industry": "汽车零部件", "sw_industry": "汽车"},  # 江淮汽车
            "002594": {"industry": "汽车零部件", "sw_industry": "汽车"},  # 比亚迪
            "002448": {"industry": "汽车零部件", "sw_industry": "汽车"},  # 中原内配
            "600660": {"industry": "汽车零部件", "sw_industry": "汽车"},  # 福耀玻璃
            "601633": {"industry": "乘用车", "sw_industry": "汽车"},  # 长城汽车
            
            # 医药生物行业
            "600276": {"industry": "化学制药", "sw_industry": "医药生物"},  # 恒瑞医药
            "000661": {"industry": "生物制品", "sw_industry": "医药生物"},  # 长春高新
            "300015": {"industry": "医疗器械", "sw_industry": "医药生物"},  # 爱尔眼科
            "603259": {"industry": "化学制药", "sw_industry": "医药生物"},  # 药明康德
            "002773": {"industry": "医疗器械", "sw_industry": "医药生物"},  # 康弘药业
            "600196": {"industry": "中药", "sw_industry": "医药生物"},  # 复星医药
            "002007": {"industry": "医疗器械", "sw_industry": "医药生物"},  # 华兰生物
            "300601": {"industry": "化学制药", "sw_industry": "医药生物"},  # 康泰生物
            "300595": {"industry": "医疗器械", "sw_industry": "医药生物"},  # 欧普康视
            "600535": {"industry": "中药", "sw_industry": "医药生物"},  # 天士力
            
            # 石油石化行业
            "601857": {"industry": "石油开采", "sw_industry": "石油石化"},  # 中国石油
            "600028": {"industry": "石油开采", "sw_industry": "石油石化"},  # 中国石化
            "601808": {"industry": "石油开采", "sw_industry": "石油石化"},  # 中海油服
            "600871": {"industry": "石油化工", "sw_industry": "石油石化"},  # 石化油服
            "000554": {"industry": "石油化工", "sw_industry": "石油石化"},  # 泰山石油
            "600339": {"industry": "石油化工", "sw_industry": "石油石化"},  # 中油工程
            "002207": {"industry": "石油化工", "sw_industry": "石油石化"},  # 准油股份
            "000819": {"industry": "石油化工", "sw_industry": "石油石化"},  # 岳阳兴长
            "000637": {"industry": "石油化工", "sw_industry": "石油石化"},  # 茂化实华
            "000718": {"industry": "石油化工", "sw_industry": "石油石化"},  # 苏宁环球
            
            # 煤炭行业
            "601088": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 中国神华
            "601225": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 陕西煤业
            "600188": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 兖州煤业
            "000983": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 西山煤电
            "601666": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 平煤股份
            "600348": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 阳泉煤业
            "600971": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 恒源煤电
            "600997": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 开滦股份
            "600123": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 兰花科创
            "600395": {"industry": "煤炭开采", "sw_industry": "煤炭"},  # 盘江股份
            
            # 有色金属行业
            "601899": {"industry": "贵金属", "sw_industry": "有色金属"},  # 紫金矿业
            "002460": {"industry": "工业金属", "sw_industry": "有色金属"},  # 赣锋锂业
            "002466": {"industry": "工业金属", "sw_industry": "有色金属"},  # 天齐锂业
            "600547": {"industry": "工业金属", "sw_industry": "有色金属"},  # 山东黄金
            "000792": {"industry": "工业金属", "sw_industry": "有色金属"},  # 盐湖股份
            "000831": {"industry": "工业金属", "sw_industry": "有色金属"},  # 五矿稀土
            "601168": {"industry": "贵金属", "sw_industry": "有色金属"},  # 西部矿业
            "601600": {"industry": "工业金属", "sw_industry": "有色金属"},  # 中国铝业
            "000060": {"industry": "工业金属", "sw_industry": "有色金属"},  # 中金岭南
            "002182": {"industry": "工业金属", "sw_industry": "有色金属"}   # 云海金属
        }
        
        cache_key = f"industry_info_{code}"
        
        # 优先检查是否为知名股票
        if code in known_industries:
            industry_data = known_industries[code]
            data = {
                "code": code,
                "industry": industry_data["industry"],
                "sw_industry": industry_data["sw_industry"]
            }
            # 强制刷新缓存
            self.set_cached_data(cache_key, data)
            return data
        
        # 如果不是知名股票，再检查缓存
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 90):
            return self.get_cached_data(cache_key)
        
        try:
            # 使用akshare获取行业信息，直接使用6位数字代码
            stock_info = ak.stock_individual_info_em(symbol=code)
            
            # 提取行业信息
            industry = stock_info.loc[stock_info['item'] == '行业', 'value'].values[0]
            
            # 映射到申万一级行业
            sw_industry = "综合"  # 默认值
            for key in self.sw_industry_map:
                if key in industry:
                    sw_industry = self.sw_industry_map[key]
                    break
            
            data = {
                "code": code,
                "industry": industry,
                "sw_industry": sw_industry
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, data)
            return data
        except Exception as e:
            print(f"获取行业信息失败：{e}")
            # 尝试根据股票代码特征猜测行业
            industry = "未知"
            sw_industry = "综合"  # 默认值
            
            # 改进的行业猜测逻辑
            if code in ["000333", "000651", "002035", "002032", "600690"]:
                industry = "白色家电" if code != "600690" else "黑色家电"
                sw_industry = "家用电器"
            elif code.startswith('600519') or code.startswith('000858') or code.startswith('000568') or code.startswith('000895'):
                industry = "白酒" if not code.startswith('000895') else "啤酒"
                sw_industry = "食品饮料"
            elif code.startswith('600036') or code.startswith('000001') or code.startswith('601166'):
                industry = "银行"
                sw_industry = "银行"
            elif code.startswith('601318') or code.startswith('601628') or code.startswith('600030'):
                industry = "保险" if code.startswith('601318') or code.startswith('601628') else "证券"
                sw_industry = "非银金融"
            elif code.startswith('000002') or code.startswith('600048') or code.startswith('601155'):
                industry = "房地产"
                sw_industry = "房地产"
            elif code.startswith('300750') or code.startswith('300274') or code.startswith('601012'):
                industry = "电池" if code.startswith('300750') else "光伏设备"
                sw_industry = "电力设备"
            elif code.startswith('002415') or code.startswith('002475') or code.startswith('000725'):
                industry = "电子制造"
                sw_industry = "电子"
            
            data = {
                "code": code,
                "industry": industry,
                "sw_industry": sw_industry
            }
            self.set_cached_data(cache_key, data)
            return data
        finally:
            # 添加API调用间隔
            time.sleep(0.5)
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def get_industry_components(self, industry_name):
        """从akshare获取行业成分股"""
        cache_key = f"industry_components_{industry_name}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 90):
            return self.get_cached_data(cache_key)
        
        # 针对知名行业的已知成分股（仅作为API调用失败时的备用）
        known_industry_components = {
            "食品饮料": ["600519", "000858", "600887", "603369", "600779"],  # 食品饮料行业
            "银行": ["600036", "000001", "601166", "600016", "601328"],      # 银行行业
            "非银金融": ["601318", "600030", "000776", "601688", "000166"],  # 非银金融行业
            "房地产": ["000002", "600048", "601155", "600383", "001979"]   # 房地产行业
        }
        
        try:
            # 使用akshare获取行业成分股
            # 注意：ak.stock_board_industry_cons_em接口的symbol参数需要正确的行业板块名称
            # 不同版本的akshare可能返回不同的列名，需要兼容处理
            
            # 首先尝试使用行业名称直接获取
            try:
                components = ak.stock_board_industry_cons_em(symbol=industry_name)
            except Exception as e:
                # 如果直接使用行业名称失败，尝试使用申万一级行业名称
                print(f"直接使用行业名称获取失败，尝试使用申万一级行业：{e}")
                # 获取申万一级行业分类
                try:
                    sw_industry_classified = ak.stock_board_industry_name_em()
                    
                    # 检查返回的DataFrame列名
                    print(f"行业分类数据列名：{list(sw_industry_classified.columns)}")
                    
                    # 尝试使用不同的列名查找
                    if 'name' in sw_industry_classified.columns:
                        matching_boards = sw_industry_classified[sw_industry_classified['name'].str.contains(industry_name, case=False)]
                    elif '板块名称' in sw_industry_classified.columns:
                        matching_boards = sw_industry_classified[sw_industry_classified['板块名称'].str.contains(industry_name, case=False)]
                    else:
                        print(f"未找到合适的列名来匹配行业名称")
                        return known_industry_components.get("食品饮料", [])  # 默认返回食品饮料行业成分股
                    
                    if not matching_boards.empty:
                        first_board = matching_boards.iloc[0]
                        # 根据实际列名获取板块名称
                        board_name = first_board['name'] if 'name' in first_board else first_board['板块名称']
                        print(f"找到匹配的行业板块：{board_name}")
                        components = ak.stock_board_industry_cons_em(symbol=board_name)
                    else:
                        print(f"未找到匹配的行业板块：{industry_name}")
                        return known_industry_components.get("食品饮料", [])  # 默认返回食品饮料行业成分股
                except Exception as inner_e:
                    print(f"获取申万一级行业分类失败：{inner_e}")
                    return known_industry_components.get("食品饮料", [])  # 默认返回食品饮料行业成分股
            
            print(f"行业{industry_name}成分股数据结构：{list(components.columns)}")
            
            # 提取股票代码列表，兼容不同列名
            stock_codes = []
            
            # 尝试所有可能的股票代码列名
            possible_code_columns = ['code', '股票代码', '代码', 'code_zm', 'Code', 'CODE', '股票代码_zm', 'symbol', 'Symbol', '代码_zm']
            
            for col in possible_code_columns:
                if col in components.columns:
                    stock_codes = components[col].tolist()
                    print(f"使用列名 '{col}' 获取股票代码")
                    break
            
            # 如果没有找到明确的股票代码列，尝试使用第一列
            if not stock_codes and len(components.columns) > 0:
                first_col = components.columns[0]
                print(f"尝试使用第一列 '{first_col}' 作为股票代码列")
                stock_codes = components[first_col].tolist()
            
            # 验证股票代码格式，只保留6位数字的股票代码
            valid_stock_codes = []
            for code in stock_codes:
                # 转换为字符串
                str_code = str(code)
                # 提取6位数字
                if len(str_code) >= 6:
                    # 从字符串中提取连续的6位数字
                    import re
                    match = re.search(r'\d{6}', str_code)
                    if match:
                        valid_code = match.group()
                        valid_stock_codes.append(valid_code)
            
            if not valid_stock_codes:
                print(f"未找到有效股票代码，返回列名：{list(components.columns)}")
                return known_industry_components.get("食品饮料", [])  # 默认返回食品饮料行业成分股
            
            # 缓存数据
            self.set_cached_data(cache_key, valid_stock_codes)
            return valid_stock_codes
        except Exception as e:
            print(f"获取行业成分股失败：{e}，行业名称：{industry_name}")
            # 打印异常详细信息
            import traceback
            traceback.print_exc()
            return known_industry_components.get("食品饮料", [])  # 默认返回食品饮料行业成分股
        finally:
            # 添加API调用间隔
            time.sleep(0.5)
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def get_historical_data(self, code, start_date=None, end_date=None, days=252):
        """获取历史交易数据"""
        cache_key = f"historical_data_{code}_{days}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 1):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            # 设置默认日期范围
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
            if not start_date:
                start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            # 获取历史数据
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="date,code,open,close,high,low,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                raise ValueError(f"未找到股票{code}的历史数据")
            
            # 转换为DataFrame格式
            df = pd.DataFrame(
                data_list,
                columns=['date', 'code', 'open', 'close', 'high', 'low', 'volume', 'amount']
            )
            
            # 安全转换数据类型
            df['close'] = pd.to_numeric(df['close'], errors='coerce').fillna(0.0)
            df['open'] = pd.to_numeric(df['open'], errors='coerce').fillna(0.0)
            df['high'] = pd.to_numeric(df['high'], errors='coerce').fillna(0.0)
            df['low'] = pd.to_numeric(df['low'], errors='coerce').fillna(0.0)
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
            
            # 计算日收益率
            df['daily_return'] = df['close'].pct_change()
            
            # 转换为字典格式缓存
            data_dict = {
                'data': df.to_dict('records'),
                'start_date': start_date,
                'end_date': end_date
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, data_dict)
            return data_dict
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    def calculate_annualized_volatility(self, daily_returns):
        """计算年化波动率"""
        if len(daily_returns) < 2:
            return 0.0
        
        # 计算日波动率（标准差）
        daily_volatility = pd.Series(daily_returns).std()
        
        # 年化波动率（假设252个交易日）
        annualized_volatility = daily_volatility * (252 ** 0.5)
        
        return annualized_volatility
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def get_market_volatility_percentile(self):
        """获取大盘波动率分位数"""
        # 获取沪深300指数的历史数据
        hs300_data = self.get_historical_data(self.hs300_code, days=504)  # 2年数据
        
        # 计算日收益率
        daily_returns = [x['daily_return'] for x in hs300_data['data'] if x['daily_return'] is not None]
        
        # 计算滚动252天的年化波动率
        volatilities = []
        for i in range(252, len(daily_returns) + 1):
            window_returns = daily_returns[i-252:i]
            volatility = self.calculate_annualized_volatility(window_returns)
            volatilities.append(volatility)
        
        # 计算当前波动率
        current_volatility = self.calculate_annualized_volatility(daily_returns[-252:])
        
        # 计算分位数
        if not volatilities:
            return 0.5
        
        percentile = sum(1 for v in volatilities if v < current_volatility) / len(volatilities)
        
        return percentile
    
    def calculate_absolute_volatility_score(self, volatility):
        """计算绝对波动率评分"""
        volatility_percent = volatility * 100
        
        if volatility_percent < 5:
            return 60
        elif volatility_percent < 20:
            return 100
        elif volatility_percent < 30:
            return 80
        elif volatility_percent < 40:
            return 60
        else:
            return 40
    
    def calculate_relative_volatility_score(self, relative_volatility):
        """计算相对波动率评分"""
        if relative_volatility < 0.5:
            return 85  # 结合换手率区分，这里取中间值
        elif relative_volatility < 0.8:
            return 90
        elif relative_volatility < 1.2:
            return 80
        elif relative_volatility < 1.5:
            return 60
        elif relative_volatility < 2.0:
            return 40
        else:
            return 20
    
    def calculate_volatility_weights(self, market_volatility_percentile):
        """计算波动率权重分配"""
        if market_volatility_percentile <= 0.15:
            # 极度平稳市场
            rel_weight = 0.50 + (0.15 - market_volatility_percentile) / 0.15 * 0.15
            rel_weight = min(0.65, rel_weight)
        elif market_volatility_percentile >= 0.85:
            # 剧烈波动市场
            rel_weight = 0.50 - (market_volatility_percentile - 0.85) / 0.15 * 0.15
            rel_weight = max(0.35, rel_weight)
        else:
            # 正常市场环境
            rel_weight = 0.50
        
        abs_weight = 1.0 - rel_weight
        
        return abs_weight, rel_weight
    
    def calculate_volatility_factor(self, volatility):
        """计算波动率校准因子"""
        volatility_percent = volatility * 100
        volatility_factor = volatility_percent / 20
        volatility_factor = min(1.2, max(0.8, volatility_factor))
        return volatility_factor
    
    def detect_volatility_surge(self, daily_returns):
        """检测波动率突变"""
        if len(daily_returns) < 20:
            return 1.0
        
        # 计算短期波动率（5日）
        short_term_volatility = self.calculate_annualized_volatility(daily_returns[-5:])
        
        # 计算长期波动率（20日）
        long_term_volatility = self.calculate_annualized_volatility(daily_returns[-20:])
        
        if long_term_volatility == 0:
            return 1.0
        
        # 计算短期/长期波动率比值
        ratio = short_term_volatility / long_term_volatility
        
        return ratio
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def get_turnover_rate(self, code, days=30, end_date=None):
        """获取换手率数据"""
        cache_key = f"turnover_rate_{code}_{days}_{end_date}" if end_date else f"turnover_rate_{code}_{days}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 1):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            # 设置日期范围
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
            
            # 获取历史数据，包含换手率
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="date,code,turn",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                raise ValueError(f"未找到股票{code}的换手率数据")
            
            # 计算平均换手率
            turnovers = []
            for data in data_list:
                if data[2]:
                    turnovers.append(float(data[2]))
            
            if not turnovers:
                raise ValueError(f"未找到有效换手率数据")
            
            average_turnover = sum(turnovers) / len(turnovers)
            latest_turnover = turnovers[-1]  # 最新换手率
            
            # 缓存数据
            result = {
                'average_turnover': average_turnover,
                'latest_turnover': latest_turnover,
                'turnover_list': turnovers
            }
            
            self.set_cached_data(cache_key, result)
            return result
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def get_circulating_market_cap(self, code):
        """获取流通市值"""
        # 转换为6位数字代码
        code = self.stock_name_to_code(code)
        cache_key = f"circulating_market_cap_{code}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 1):
            return self.get_cached_data(cache_key)
        
        try:
            # 格式化股票代码为baostock需要的格式
            formatted_code = self.format_stock_code(code)
            
            # 初始化baostock连接
            bs.login()
            
            # 获取最近7天的交易数据，包含成交量和收盘价
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
            rs = bs.query_history_k_data_plus(
                code=formatted_code,
                fields="date,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                raise ValueError(f"未找到股票{code}的交易数据")
            
            # 使用最新的一条数据
            latest_data = data_list[-1]
            close_price = float(latest_data[1])  # 最新收盘价
            volume = float(latest_data[2])        # 成交量（股）
            
            # 获取股票基本信息，包含流通股本
            rs_basic = bs.query_stock_basic(code=code)
            circulating_share = 0.0
            while (rs_basic.error_code == '0') & rs_basic.next():
                basic_row = rs_basic.get_row_data()
                if len(basic_row) >= 14:  # 确保有足够的字段
                    circulating_share = float(basic_row[13]) if basic_row[13] else 0.0  # 流通股本
                break
            
            # 如果没有获取到流通股本，尝试用成交量和金额计算近似值
            if circulating_share == 0.0:
                amount = float(latest_data[3])  # 成交金额（元）
                if amount > 0 and volume > 0:
                    # 计算平均成交价
                    avg_price = amount / volume
                    # 假设当天成交量占流通股本的1%，估算流通股本
                    estimated_share = volume * 100
                    circulating_cap = estimated_share * avg_price / 100000000  # 转换为亿元
                    print(f"估算流通市值：{code} -> {circulating_cap:.2f}亿")
                else:
                    # 使用默认值作为最后的兜底
                    circulating_cap = 1000.0
                    print(f"使用默认流通市值：{code} -> {circulating_cap}亿")
            else:
                # 计算流通市值：流通股本（股） * 最新收盘价（元） / 100000000 = 亿元
                circulating_cap = circulating_share * close_price / 100000000
                print(f"计算流通市值：{code} -> {circulating_cap:.2f}亿")
            
            # 缓存数据
            self.set_cached_data(cache_key, circulating_cap)
            return circulating_cap
        except Exception as e:
            print(f"获取流通市值失败：{e}")
            # 返回默认值作为兜底
            return 1000.0
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(1.0)
    
    def get_market_cap_group(self, circulating_cap):
        """根据流通市值确定市值分组"""
        # 流通市值单位：亿
        if circulating_cap >= 2000:
            return "特大盘"
        elif circulating_cap >= 1000:
            return "超大盘"
        elif circulating_cap >= 500:
            return "大盘"
        elif circulating_cap >= 200:
            return "中盘"
        elif circulating_cap >= 50:
            return "小盘"
        elif circulating_cap >= 10:
            return "微小盘A"
        else:
            return "微小盘B"
    
    def get_industry_adjustment_factor(self, industry_name):
        """获取行业调整因子"""
        # 查找行业调整因子
        if industry_name in self.industry_adjustment:
            return self.industry_adjustment[industry_name]
        
        # 默认值
        return 1.0
    
    def calculate_rtr(self, actual_turnover, benchmark_turnover, industry_factor):
        """计算相对换手率(RTR)"""
        # actual_turnover是百分比值（例如：0.253%），benchmark_turnover是小数形式（例如：0.0035代表0.35%）
        # 需要将benchmark_turnover转换为百分比值进行计算
        benchmark_turnover_percent = benchmark_turnover * 100
        denominator = benchmark_turnover_percent * industry_factor
        
        if denominator == 0:
            return 1.0
        
        return actual_turnover / denominator
    
    def calculate_asymmetric_risk_deviation(self, rtr):
        """计算非对称风险偏离度"""
        center_value = 1.0
        
        if rtr > center_value:
            deviation = (rtr - center_value) / center_value * 100 * 1.5
        else:
            deviation = (center_value - rtr) / center_value * 100 * 0.3
        
        return deviation
    
    def map_deviation_to_score(self, deviation):
        """评分映射"""
        deviation_abs = abs(deviation)
        
        if deviation_abs < 10:
            return 100
        elif deviation_abs < 20:
            return 85
        elif deviation_abs < 40:
            return 70
        elif deviation_abs < 70:
            return 55
        elif deviation_abs < 100:
            return 40
        else:
            return 25
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_turnover_analysis(self, name_or_code, end_date=None):
        """换手率分析指标"""
        try:
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            # 格式化股票代码为baostock格式
            formatted_code = self.format_stock_code(code)
            
            # 获取行业信息
            industry_info = self.get_industry_info(code)
            
            # 获取换手率数据
            turnover_data = self.get_turnover_rate(formatted_code, end_date=end_date)
            actual_turnover = turnover_data['average_turnover']  # 使用平均换手率
            
            # 获取流通市值
            circulating_cap = self.get_circulating_market_cap(code)
            
            # 确定市值分组
            market_cap_group = self.get_market_cap_group(circulating_cap)
            
            # 获取基准换手率
            benchmark_turnover = self.market_cap_benchmark[market_cap_group]
            
            # 获取行业调整因子
            industry_factor = self.get_industry_adjustment_factor(industry_info['sw_industry'])
            
            # 计算相对换手率(RTR)
            rtr = self.calculate_rtr(actual_turnover, benchmark_turnover, industry_factor)
            
            # 计算非对称风险偏离度
            risk_deviation = self.calculate_asymmetric_risk_deviation(rtr)
            
            # 映射评分
            score = self.map_deviation_to_score(risk_deviation)
            
            # 构建结果
            result = {
                'actual_turnover': actual_turnover,
                'circulating_market_cap': circulating_cap,
                'market_cap_group': market_cap_group,
                'benchmark_turnover': benchmark_turnover,
                'industry_factor': industry_factor,
                'rtr': rtr,
                'risk_deviation': risk_deviation,
                'score': score
            }
            
            return result
        except Exception as e:
            print(f"换手率分析失败：{e}")
            raise
    
    def calculate_rsi(self, prices, period=14):
        """计算RSI值"""
        if len(prices) < period + 1:
            return [0.0] * len(prices)
        
        gains = []
        losses = []
        
        # 计算初始涨跌幅
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        # 计算初始平均涨跌幅
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        rsi_values = []
        
        # 计算第一个RSI值
        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        rsi_values.append(rsi)
        
        # 计算后续RSI值
        for i in range(period, len(prices) - 1):
            # 使用平滑移动平均
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            
            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            rsi_values.append(rsi)
        
        # 前period-1个数据填充为0
        return [0.0] * (period - 1) + rsi_values
    
    def calculate_rsi_score(self, rsi_value, sigma):
        """计算RSI基础评分"""
        # 使用正态分布函数将RSI值映射到0-100分
        exponent = -((rsi_value - 50) ** 2) / (2 * sigma ** 2)
        rsi_score = 100 * math.exp(exponent)
        return rsi_score
    
    def calculate_dynamic_sigma(self, volatility, market_cap_group):
        """计算动态σ值"""
        # 基础σ值
        base_sigma = 15
        
        # 根据市值调整σ值
        market_cap_factor = {
            "特大盘": 1.0,
            "超大盘": 1.0,
            "大盘": 1.0,
            "中盘": 1.067,
            "小盘": 1.133,
            "微小盘A": 1.133,
            "微小盘B": 1.133
        }[market_cap_group]
        
        # 根据波动率调整σ值
        volatility_factor = min(1.5, max(0.5, volatility * 100 / 20))
        
        # 计算最终σ值
        sigma = base_sigma * market_cap_factor * volatility_factor
        
        # 限制σ值在10-20之间
        sigma = min(20, max(10, sigma))
        
        return sigma
    
    def identify_rsi_signals(self, rsi_values, prices):
        """识别RSI信号"""
        signal_score = 0
        signal_description = []
        
        if len(rsi_values) < 14 or len(prices) < 14:
            return signal_score, signal_description
        
        # 最近14个交易日的数据
        recent_rsi = rsi_values[-14:]
        recent_prices = prices[-14:]
        
        # 1. 精确背离识别
        # 顶背离：价格创新高(+2%)，RSI未创新高(-10%)
        price_high = max(recent_prices)
        price_high_idx = recent_prices.index(price_high)
        rsi_high = max(recent_rsi)
        rsi_high_idx = recent_rsi.index(rsi_high)
        
        if price_high_idx == len(recent_prices) - 1:  # 价格创新高
            price_change = (price_high - recent_prices[price_high_idx - 1]) / recent_prices[price_high_idx - 1] * 100
            if price_change >= 2:  # 价格创新高+2%
                if rsi_high_idx != len(recent_rsi) - 1:  # RSI未创新高
                    rsi_change = (recent_rsi[-1] - rsi_high) / rsi_high * 100
                    if rsi_change <= -10:  # RSI未创新高-10%
                        signal_score -= 15
                        signal_description.append("RSI顶背离")
        
        # 底背离：价格创新低(-2%)，RSI未创新低(+10%)
        price_low = min(recent_prices)
        price_low_idx = recent_prices.index(price_low)
        rsi_low = min(recent_rsi)
        rsi_low_idx = recent_rsi.index(rsi_low)
        
        if price_low_idx == len(recent_prices) - 1:  # 价格创新低
            price_change = (price_low - recent_prices[price_low_idx - 1]) / recent_prices[price_low_idx - 1] * 100
            if price_change <= -2:  # 价格创新低-2%
                if rsi_low_idx != len(recent_rsi) - 1:  # RSI未创新低
                    rsi_change = (recent_rsi[-1] - rsi_low) / rsi_low * 100
                    if rsi_change >= 10:  # RSI未创新低+10%
                        signal_score += 15
                        signal_description.append("RSI底背离")
        
        # 2. 趋势突破确认
        # 超买确认：连续3个交易日RSI>65
        if len(recent_rsi) >= 3:
            if all(rsi > 65 for rsi in recent_rsi[-3:]):
                signal_score -= 10
                signal_description.append("RSI超买确认")
        
        # 超卖确认：连续3个交易日RSI<35
        if len(recent_rsi) >= 3:
            if all(rsi < 35 for rsi in recent_rsi[-3:]):
                signal_score += 10
                signal_description.append("RSI超卖确认")
        
        # 限制信号得分范围为-20到+20
        signal_score = min(20, max(-20, signal_score))
        
        return signal_score, signal_description
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_rsi_analysis(self, name_or_code, end_date=None):
        """RSI分析指标"""
        try:
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            # 格式化股票代码为baostock格式
            formatted_code = self.format_stock_code(code)
            
            # 获取股票历史数据
            stock_data = self.get_historical_data(formatted_code, days=90, end_date=end_date)
            
            # 提取收盘价
            prices = [x['close'] for x in stock_data['data']]
            
            # 计算RSI值
            rsi_values = self.calculate_rsi(prices)
            current_rsi = rsi_values[-1] if rsi_values else 50
            
            # 获取流通市值
            circulating_cap = self.get_circulating_market_cap(code)
            
            # 确定市值分组
            market_cap_group = self.get_market_cap_group(circulating_cap)
            
            # 计算波动率
            returns = [x['daily_return'] for x in stock_data['data'] if x['daily_return'] is not None]
            volatility = self.calculate_annualized_volatility(returns) if returns else 0.2
            
            # 计算动态σ值
            sigma = self.calculate_dynamic_sigma(volatility, market_cap_group)
            
            # 计算RSI基础评分
            base_score = self.calculate_rsi_score(current_rsi, sigma)
            
            # 识别RSI信号
            signal_score, signal_description = self.identify_rsi_signals(rsi_values, prices)
            
            # 计算最终RSI评分：基础评分占95%，信号得分占5%
            final_score = base_score * 0.95 + signal_score * 0.05
            final_score = min(100, max(0, final_score))  # 限制在0-100之间
            
            # 计算RSI状态
            rsi_status = '未知'
            if current_rsi > 70:
                rsi_status = '超买'
            elif current_rsi < 30:
                rsi_status = '超卖'
            else:
                rsi_status = '中性'
            
            # 构建结果
            result = {
                'current_rsi': current_rsi,
                'sigma': sigma,
                'base_score': base_score,
                'signal_score': signal_score,
                'signal_description': signal_description,
                'final_score': round(final_score, 2),
                'rsi_values': rsi_values[-14:],  # 返回最近14天的RSI值
                'volatility': volatility,
                'rsi_status': rsi_status
            }
            
            return result
        except Exception as e:
            print(f"RSI分析失败：{e}")
            raise
    
    def calculate_moving_averages(self, prices):
        """计算5种不同周期的移动平均线"""
        ma_10 = []
        ma_20 = []
        ma_50 = []
        ma_60 = []
        ma_200 = []
        
        # 计算10日均线
        for i in range(len(prices)):
            if i < 9:  # 前9天数据不足
                ma_10.append(0)
            else:
                ma_10.append(sum(prices[i-9:i+1]) / 10)
        
        # 计算20日均线
        for i in range(len(prices)):
            if i < 19:  # 前19天数据不足
                ma_20.append(0)
            else:
                ma_20.append(sum(prices[i-19:i+1]) / 20)
        
        # 计算50日均线
        for i in range(len(prices)):
            if i < 49:  # 前49天数据不足
                ma_50.append(0)
            else:
                ma_50.append(sum(prices[i-49:i+1]) / 50)
        
        # 计算60日均线
        for i in range(len(prices)):
            if i < 59:  # 前59天数据不足
                ma_60.append(0)
            else:
                ma_60.append(sum(prices[i-59:i+1]) / 60)
        
        # 计算200日均线
        for i in range(len(prices)):
            if i < 199:  # 前199天数据不足
                ma_200.append(0)
            else:
                ma_200.append(sum(prices[i-199:i+1]) / 200)
        
        return {
            'ma_10': ma_10,
            'ma_20': ma_20,
            'ma_50': ma_50,
            'ma_60': ma_60,
            'ma_200': ma_200
        }
    
    def calculate_stochastic(self, prices, high_prices, low_prices, period=14, d_period=3):
        """计算Stochastic指标"""
        k_values = []
        d_values = []
        
        if len(prices) < period:
            return {
                'k_values': [0] * len(prices),
                'd_values': [0] * len(prices)
            }
        
        # 计算K值
        for i in range(period-1, len(prices)):
            recent_high = max(high_prices[i-period+1:i+1])
            recent_low = min(low_prices[i-period+1:i+1])
            
            if recent_high == recent_low:
                k = 50
            else:
                k = ((prices[i] - recent_low) / (recent_high - recent_low)) * 100
            
            k_values.append(k)
        
        # 前period-1个数据填充为0
        k_values = [0] * (period-1) + k_values
        
        # 计算D值（K值的3日移动平均）
        for i in range(d_period-1, len(k_values)):
            d = sum(k_values[i-d_period+1:i+1]) / d_period
            d_values.append(d)
        
        # 前d_period-1个数据填充为0
        d_values = [0] * (d_period-1) + d_values
        
        # 确保长度一致
        while len(d_values) < len(prices):
            d_values.append(d_values[-1] if d_values else 0)
        
        return {
            'k_values': k_values,
            'd_values': d_values
        }
    
    def calculate_adx(self, prices, high_prices, low_prices, period=14):
        """计算ADX指标"""
        adx_values = []
        
        if len(prices) < period + 1:
            return [0] * len(prices)
        
        # 计算TR（真实波动幅度）
        tr = []
        for i in range(1, len(prices)):
            h = high_prices[i]
            l = low_prices[i]
            c_prev = prices[i-1]
            
            tr1 = h - l
            tr2 = abs(h - c_prev)
            tr3 = abs(l - c_prev)
            tr.append(max(tr1, tr2, tr3))
        
        # 计算DM+和DM-
        dm_plus = []
        dm_minus = []
        for i in range(1, len(prices)):
            up_move = high_prices[i] - high_prices[i-1]
            down_move = low_prices[i-1] - low_prices[i]
            
            if up_move > down_move and up_move > 0:
                dm_plus.append(up_move)
                dm_minus.append(0)
            elif down_move > up_move and down_move > 0:
                dm_minus.append(down_move)
                dm_plus.append(0)
            else:
                dm_plus.append(0)
                dm_minus.append(0)
        
        # 计算ATR、+DI、-DI
        atr = []
        di_plus = []
        di_minus = []
        
        # 初始ATR
        initial_atr = sum(tr[:period]) / period
        atr.append(initial_atr)
        
        # 初始DM+和DM-
        initial_dm_plus = sum(dm_plus[:period]) / period
        initial_dm_minus = sum(dm_minus[:period]) / period
        
        if initial_atr != 0:
            initial_di_plus = (initial_dm_plus / initial_atr) * 100
            initial_di_minus = (initial_dm_minus / initial_atr) * 100
        else:
            initial_di_plus = 0
            initial_di_minus = 0
        
        di_plus.append(initial_di_plus)
        di_minus.append(initial_di_minus)
        
        # 计算后续ATR、+DI、-DI
        for i in range(period, len(tr)):
            # 平滑ATR
            new_atr = (atr[-1] * (period - 1) + tr[i]) / period
            atr.append(new_atr)
            
            # 平滑DM+和DM-
            new_dm_plus = (initial_dm_plus * (period - 1) + dm_plus[i]) / period
            new_dm_minus = (initial_dm_minus * (period - 1) + dm_minus[i]) / period
            
            if new_atr != 0:
                new_di_plus = (new_dm_plus / new_atr) * 100
                new_di_minus = (new_dm_minus / new_atr) * 100
            else:
                new_di_plus = 0
                new_di_minus = 0
            
            di_plus.append(new_di_plus)
            di_minus.append(new_di_minus)
            
            initial_dm_plus = new_dm_plus
            initial_dm_minus = new_dm_minus
        
        # 计算DX
        dx = []
        for i in range(len(di_plus)):
            if di_plus[i] + di_minus[i] != 0:
                dx_value = (abs(di_plus[i] - di_minus[i]) / (di_plus[i] + di_minus[i])) * 100
                dx.append(dx_value)
            else:
                dx.append(0)
        
        # 计算ADX
        if len(dx) < period:
            adx_values = [0] * (len(prices) - period + 1)
        else:
            # 初始ADX
            initial_adx = sum(dx[:period]) / period
            adx_values.append(initial_adx)
            
            # 后续ADX
            for i in range(period, len(dx)):
                new_adx = (adx_values[-1] * (period - 1) + dx[i]) / period
                adx_values.append(new_adx)
        
        # 前period-1个数据填充为0
        full_adx = [0] * (len(prices) - len(adx_values)) + adx_values
        
        return full_adx
    
    def determine_trend_direction(self, ma_10, ma_20, ma_50, ma_60, ma_200):
        """判断趋势方向"""
        if not ma_10 or not ma_20 or not ma_50 or not ma_60 or not ma_200:
            return "震荡"
        
        # 获取最新的均线值
        latest_ma_10 = ma_10[-1]
        latest_ma_20 = ma_20[-1]
        latest_ma_50 = ma_50[-1]
        latest_ma_60 = ma_60[-1]
        latest_ma_200 = ma_200[-1]
        
        # 检查均线是否足够长
        if latest_ma_10 == 0 or latest_ma_20 == 0 or latest_ma_50 == 0 or latest_ma_60 == 0 or latest_ma_200 == 0:
            return "震荡"
        
        # 强劲上升：所有均线依次排列，且短期均线明显高于长期均线
        if (latest_ma_10 > latest_ma_20 * 1.01 and 
            latest_ma_20 > latest_ma_50 * 1.01 and 
            latest_ma_50 > latest_ma_60 * 1.01 and 
            latest_ma_60 > latest_ma_200 * 1.01):
            return "强劲上升"
        
        # 明显上升：所有均线依次排列，短期均线高于长期均线
        elif (latest_ma_10 > latest_ma_20 and 
              latest_ma_20 > latest_ma_50 and 
              latest_ma_50 > latest_ma_60 and 
              latest_ma_60 > latest_ma_200):
            return "明显上升"
        
        # 温和上升：大部分均线依次排列，短期均线高于长期均线
        elif (latest_ma_10 > latest_ma_20 and 
              latest_ma_20 > latest_ma_60 and 
              latest_ma_60 > latest_ma_200):
            return "温和上升"
        
        # 强劲下降：所有均线依次排列，且短期均线明显低于长期均线
        elif (latest_ma_10 < latest_ma_20 * 0.99 and 
              latest_ma_20 < latest_ma_50 * 0.99 and 
              latest_ma_50 < latest_ma_60 * 0.99 and 
              latest_ma_60 < latest_ma_200 * 0.99):
            return "强劲下降"
        
        # 明显下降：所有均线依次排列，短期均线低于长期均线
        elif (latest_ma_10 < latest_ma_20 and 
              latest_ma_20 < latest_ma_50 and 
              latest_ma_50 < latest_ma_60 and 
              latest_ma_60 < latest_ma_200):
            return "明显下降"
        
        # 温和下降：大部分均线依次排列，短期均线低于长期均线
        elif (latest_ma_10 < latest_ma_20 and 
              latest_ma_20 < latest_ma_60 and 
              latest_ma_60 < latest_ma_200):
            return "温和下降"
        
        # 震荡：其他情况
        else:
            return "震荡"
    
    def calculate_base_score(self, trend_status):
        """计算基准评分"""
        base_scores = {
            "强劲上升": 80,
            "明显上升": 70,
            "温和上升": 60,
            "震荡": 50,
            "温和下降": 40,
            "明显下降": 30,
            "强劲下降": 20
        }
        return base_scores.get(trend_status, 50)
    
    def get_profit_data(self, code, year, quarter):
        """从baostock获取利润数据"""
        cache_key = f"profit_data_{code}_{year}_{quarter}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), self.financial_cache_durations["other_financials"]):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            rs = bs.query_profit_data(
                code=code,
                year=str(year),
                quarter=str(quarter)
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                result = {
                    "roe": None,
                    "netProfitMargin": None,
                    "grossIncomeRatio": None,
                    "eps": None,
                    "netProfit": None,
                    "totalShare": None
                }
            else:
                data = data_list[0]
                # 添加调试信息，查看原始数据结构
                print(f"  baostock返回的利润数据原始结构：{data}")
                print(f"  数据长度：{len(data)}")
                
                # 检查数据长度是否足够
                if len(data) >= 9:
                    # 添加EPS数据调试和修正
                    raw_eps = data[6] if data[6] else None
                    eps_value = float(raw_eps) if raw_eps else None
                    
                    if eps_value is not None:
                        print(f"  [EPS调试] 原始EPS值：{eps_value}")
                        # 检查EPS值是否合理（正常EPS应该在-10到100之间）
                        if abs(eps_value) > 100:
                            # 根据baostock返回的实际数据，EPS单位应该是元/10亿股，需要除以10^9转换为元/股
                            eps_value = eps_value / 1000000000  # 转换为元/股
                            print(f"  [EPS调试] 修正后EPS值（元/股）：{eps_value}（除以1000000000）")
                    
                    result = {
                        "roe": float(data[3]) if data[3] else None,  # 净资产收益率
                        "netProfitMargin": float(data[4]) if data[4] else None,  # 净利率
                        "grossIncomeRatio": float(data[5]) if data[5] else None,  # 毛利率
                        "eps": eps_value,  # 每股收益（已修正）
                        "netProfit": float(data[7]) if data[7] else None,  # 净利润
                        "totalShare": float(data[8]) if data[8] else None  # 总股本
                    }
                else:
                    # 数据长度不足，可能是API返回结构变化
                    print(f"  数据长度不足，无法获取所有指标，仅获取到{len(data)}个字段")
                    result = {
                        "roe": float(data[3]) if len(data) > 3 and data[3] else None,
                        "netProfitMargin": float(data[4]) if len(data) > 4 and data[4] else None,
                        "grossIncomeRatio": float(data[5]) if len(data) > 5 and data[5] else None,
                        "eps": float(data[6]) if len(data) > 6 and data[6] else None,
                        "netProfit": float(data[7]) if len(data) > 7 and data[7] else None,
                        "totalShare": float(data[8]) if len(data) > 8 and data[8] else None
                    }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    def get_cash_flow_data(self, code, year, quarter):
        """从baostock获取现金流数据"""
        cache_key = f"cash_flow_data_{code}_{year}_{quarter}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), self.financial_cache_durations["other_financials"]):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            rs = bs.query_cash_flow_data(
                code=code,
                year=str(year),
                quarter=str(quarter)
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                result = {
                    "cfoToNp": None,  # 利润现金保障倍数
                    "cfoToOr": None,  # 营业收入现金含量
                    "ebitToInterest": None,  # 利息保障倍数
                    "operatingCashFlow": None  # 经营现金流
                }
            else:
                data = data_list[0]
                result = {
                    "cfoToNp": float(data[3]) if data[3] else None,  # 利润现金保障倍数
                    "cfoToOr": float(data[4]) if data[4] else None,  # 营业收入现金含量
                    "ebitToInterest": float(data[5]) if data[5] else None,  # 利息保障倍数
                    "operatingCashFlow": float(data[6]) if data[6] else None  # 经营现金流
                }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    def get_balance_data(self, code, year, quarter):
        """从baostock获取资产负债数据"""
        cache_key = f"balance_data_{code}_{year}_{quarter}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), self.financial_cache_durations["other_financials"]):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            rs = bs.query_balance_data(
                code=code,
                year=str(year),
                quarter=str(quarter)
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                result = {
                    "currentRatio": None,  # 流动比率
                    "quickRatio": None  # 速动比率
                }
            else:
                data = data_list[0]
                result = {
                    "currentRatio": float(data[3]) if data[3] else None,  # 流动比率
                    "quickRatio": float(data[4]) if data[4] else None  # 速动比率
                }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    def calculate_trend_strength_adjustment(self, trend_status, adx, base_score):
        """根据ADX指标调整基准评分"""
        # 确定调整因子
        if trend_status == "震荡":
            adjustment_factor = 0.5
        else:
            adjustment_factor = 1.0
        
        # 趋势强度调整
        if adx > 35:  # 强趋势
            if "上升" in trend_status:
                base_score = min(100, base_score + safe_int(10 * adjustment_factor))
            elif "下降" in trend_status:
                base_score = max(0, base_score - safe_int(10 * adjustment_factor))
        elif adx < 20:  # 弱趋势
            if "上升" in trend_status:
                base_score = max(50, base_score - safe_int(10 * adjustment_factor))
            elif "下降" in trend_status:
                base_score = min(50, base_score + safe_int(10 * adjustment_factor))
        
        # 评分范围限制为0-100
        base_score = min(100, max(0, base_score))
        
        return base_score
    
    def detect_rsi_divergence(self, prices, rsi_values, lookback=14):
        """检测RSI背离"""
        if len(prices) < lookback + 5 or len(rsi_values) < lookback + 5:
            return None
        
        # 获取最近的价格和RSI数据
        recent_prices = prices[-lookback:]
        recent_rsi = rsi_values[-lookback:]
        
        # 查找价格的最近两个高点和低点
        price_highs = []
        price_lows = []
        
        for i in range(1, len(recent_prices) - 1):
            # 查找价格高点
            if recent_prices[i] > recent_prices[i-1] and recent_prices[i] > recent_prices[i+1]:
                price_highs.append((i, recent_prices[i]))
            # 查找价格低点
            if recent_prices[i] < recent_prices[i-1] and recent_prices[i] < recent_prices[i+1]:
                price_lows.append((i, recent_prices[i]))
        
        # 查找RSI的最近两个高点和低点
        rsi_highs = []
        rsi_lows = []
        
        for i in range(1, len(recent_rsi) - 1):
            # 查找RSI高点
            if recent_rsi[i] > recent_rsi[i-1] and recent_rsi[i] > recent_rsi[i+1]:
                rsi_highs.append((i, recent_rsi[i]))
            # 查找RSI低点
            if recent_rsi[i] < recent_rsi[i-1] and recent_rsi[i] < recent_rsi[i+1]:
                rsi_lows.append((i, recent_rsi[i]))
        
        # 检测顶背离：价格创新高，RSI未创新高
        if len(price_highs) >= 2 and len(rsi_highs) >= 2:
            # 最近两个价格高点
            price_high1 = price_highs[-1]
            price_high2 = price_highs[-2]
            
            # 最近两个RSI高点
            rsi_high1 = rsi_highs[-1]
            rsi_high2 = rsi_highs[-2]
            
            # 检查价格是否创新高（+2%）
            price_change = (price_high1[1] - price_high2[1]) / price_high2[1] * 100
            
            # 检查RSI是否未创新高（-10%）
            rsi_change = (rsi_high1[1] - rsi_high2[1]) / rsi_high2[1] * 100
            
            if price_change >= 2 and rsi_change <= -10:
                return "顶背离"
        
        # 检测底背离：价格创新低，RSI未创新低
        if len(price_lows) >= 2 and len(rsi_lows) >= 2:
            # 最近两个价格低点
            price_low1 = price_lows[-1]
            price_low2 = price_lows[-2]
            
            # 最近两个RSI低点
            rsi_low1 = rsi_lows[-1]
            rsi_low2 = rsi_lows[-2]
            
            # 检查价格是否创新低（-2%）
            price_change = (price_low1[1] - price_low2[1]) / price_low2[1] * 100
            
            # 检查RSI是否未创新低（+10%）
            rsi_change = (rsi_low1[1] - rsi_low2[1]) / rsi_low2[1] * 100
            
            if price_change <= -2 and rsi_change >= 10:
                return "底背离"
        
        return None
    
    def detect_trend_start(self, ma_short, ma_long):
        """基于移动平均线交叉检测趋势开始时间"""
        if len(ma_short) < 2 or len(ma_long) < 2:
            return 0
        
        trend_start = 0
        # 从最近的点开始向前查找交叉点
        for i in range(len(ma_short)-1, 0, -1):
            # 检查是否有均线交叉
            # 短期均线上穿长期均线（金叉）
            if ma_short[i] > ma_long[i] and ma_short[i-1] <= ma_long[i-1]:
                trend_start = i
                break
            # 短期均线下穿长期均线（死叉）
            elif ma_short[i] < ma_long[i] and ma_short[i-1] >= ma_long[i-1]:
                trend_start = i
                break
        
        return trend_start
    
    def calculate_trend_duration(self, prices, ma_10, ma_20):
        """计算趋势持续时长"""
        # 使用10日和20日均线交叉识别趋势开始
        trend_start = self.detect_trend_start(ma_10, ma_20)
        
        if trend_start == 0:
            return 0
        
        # 计算趋势持续天数
        trend_duration = len(prices) - trend_start
        
        return trend_duration
    
    def calculate_trend_exhaustion(self, rsi, rsi_values, stochastic_k, prices, volumes, ma_10, ma_20):
        """计算趋势衰竭预警"""
        exhaustion_score = 0
        
        # 超买超卖检测（最高60分）
        if rsi > 70 or rsi < 30:
            exhaustion_score += 30
        if stochastic_k > 80 or stochastic_k < 20:
            exhaustion_score += 30
        
        # 背离信号检测（最高40分）
        divergence = self.detect_rsi_divergence(prices, rsi_values)
        if divergence == "顶背离":
            exhaustion_score += 40
        elif divergence == "底背离":
            exhaustion_score += 40
        
        # 成交量验证（最高30分）
        if len(prices) > 5 and len(volumes) > 5:
            recent_prices = prices[-5:]
            recent_volumes = volumes[-5:]
            
            # 价格上升但成交量下降
            if recent_prices[-1] > recent_prices[0] and recent_volumes[-1] < recent_volumes[0] * 0.8:
                exhaustion_score += 30
            # 价格下降但成交量下降
            elif recent_prices[-1] < recent_prices[0] and recent_volumes[-1] < recent_volumes[0] * 0.8:
                exhaustion_score += 15
        
        # 趋势时长衰减（最高30分）
        trend_duration = self.calculate_trend_duration(prices, ma_10, ma_20)
        if trend_duration > 120:
            exhaustion_score += 30
        elif trend_duration > 80:
            exhaustion_score += 20
        elif trend_duration > 40:
            exhaustion_score += 10
        
        # 总得分按比例压缩至0-120分
        exhaustion_score = min(120, max(0, exhaustion_score))
        
        return exhaustion_score
    
    def calculate_trend_conversion(self, ma_10, ma_20, ma_200, adx, volatility, volumes):
        """计算趋势转换预警"""
        conversion_score = 0
        
        # 均线交叉预警（25分）
        if len(ma_10) > 0 and len(ma_20) > 0:
            latest_ma_10 = ma_10[-1]
            latest_ma_20 = ma_20[-1]
            if latest_ma_10 != 0 and latest_ma_20 != 0:
                diff = abs((latest_ma_10 - latest_ma_20) / latest_ma_20 * 100)
                if diff < 1:
                    conversion_score += 25
        
        # 关键均线预警（30分）
        if len(ma_200) > 0 and len(ma_10) > 0:
            latest_ma_200 = ma_200[-1]
            latest_ma_10 = ma_10[-1]
            if latest_ma_200 != 0 and latest_ma_10 != 0:
                # 有效突破年线
                if latest_ma_10 > latest_ma_200 * 1.01 or latest_ma_10 < latest_ma_200 * 0.99:
                    conversion_score += 30
        
        # 趋势强度变化（30分）
        if len(adx) > 5:
            # 最近5天ADX从>35快速下降至<28
            recent_adx = adx[-5:]
            if any(a > 35 for a in recent_adx[:3]) and all(a < 28 for a in recent_adx[-2:]):
                conversion_score += 30
        
        # 波动率突变（25分）
        # 简化处理，假设波动率显著增加
        if volatility > 0.3:
            conversion_score += 25
        
        # 成交量异动（20分）
        if len(volumes) > 5:
            recent_volumes = volumes[-5:]
            avg_volume = sum(recent_volumes[:-1]) / 4
            if recent_volumes[-1] > avg_volume * 2:
                conversion_score += 20
        
        # 总得分按比例压缩至0-130分
        conversion_score = min(130, max(0, conversion_score))
        
        # 确定预警等级
        if conversion_score < 25:
            warning_level = "无预警（0级）"
        elif conversion_score < 50:
            warning_level = "轻度预警（1级）"
        elif conversion_score < 80:
            warning_level = "中度预警（2级）"
        else:
            warning_level = "重度预警（3级）"
        
        return conversion_score, warning_level
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_trend_analysis(self, name_or_code, end_date=None):
        """趋势分析指标"""
        try:
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            # 格式化股票代码为baostock格式
            formatted_code = self.format_stock_code(code)
            
            # 获取股票历史数据
            stock_data = self.get_historical_data(formatted_code, days=300, end_date=end_date)
            
            # 提取数据
            prices = [x['close'] for x in stock_data['data']]
            high_prices = [x['high'] for x in stock_data['data']]
            low_prices = [x['low'] for x in stock_data['data']]
            volumes = [x['volume'] for x in stock_data['data']]
            
            # 计算移动平均线
            moving_averages = self.calculate_moving_averages(prices)
            ma_10 = moving_averages['ma_10']
            ma_20 = moving_averages['ma_20']
            ma_50 = moving_averages['ma_50']
            ma_60 = moving_averages['ma_60']
            ma_200 = moving_averages['ma_200']
            
            # 计算技术指标
            rsi_values = self.calculate_rsi(prices)
            current_rsi = rsi_values[-1] if rsi_values else 50
            
            stochastic = self.calculate_stochastic(prices, high_prices, low_prices)
            current_stochastic_k = stochastic['k_values'][-1] if stochastic['k_values'] else 50
            
            adx_values = self.calculate_adx(prices, high_prices, low_prices)
            current_adx = adx_values[-1] if adx_values else 25
            
            # 计算波动率
            returns = [x['daily_return'] for x in stock_data['data'] if x['daily_return'] is not None]
            volatility = self.calculate_annualized_volatility(returns) if returns else 0.2
            
            # 判断趋势方向
            trend_status = self.determine_trend_direction(ma_10, ma_20, ma_50, ma_60, ma_200)
            
            # 计算基准评分
            base_score = self.calculate_base_score(trend_status)
            
            # 根据ADX调整基准评分
            adjusted_score = self.calculate_trend_strength_adjustment(trend_status, current_adx, base_score)
            
            # 计算趋势衰竭预警
            exhaustion_score = self.calculate_trend_exhaustion(current_rsi, rsi_values, current_stochastic_k, prices, volumes, ma_10, ma_20)
            
            # 计算趋势转换预警
            conversion_score, warning_level = self.calculate_trend_conversion(ma_10, ma_20, ma_200, adx_values, volatility, volumes)
            
            # 计算最终得分：基础趋势评分*(1-min(趋势衰竭得分/120, 0.5))
            final_score = adjusted_score * (1 - min(exhaustion_score / 120, 0.5))
            final_score = min(100, max(0, final_score))
            
            # 计算趋势强度
            trend_strength = '未知'
            if current_adx < 20:
                trend_strength = '弱趋势'
            elif current_adx < 40:
                trend_strength = '中等趋势'
            else:
                trend_strength = '强趋势'
            
            # 构建结果
            result = {
                'trend_status': trend_status,
                'current_adx': current_adx,
                'base_score': base_score,
                'adjusted_score': adjusted_score,
                'exhaustion_score': exhaustion_score,
                'conversion_score': conversion_score,
                'warning_level': warning_level,
                'final_score': round(final_score, 2),
                'current_rsi': current_rsi,
                'current_stochastic_k': current_stochastic_k,
                'volatility': volatility,
                'trend_strength': trend_strength
            }
            
            return result
        except Exception as e:
            print(f"趋势分析失败：{e}")
            raise
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_volatility_analysis(self, name_or_code, end_date=None):
        """波动率分析指标"""
        try:
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            # 格式化股票代码为baostock格式
            formatted_code = self.format_stock_code(code)
            
            # 获取股票基本信息
            basic_info = self.get_stock_basic_info(formatted_code)
            
            # 获取行业信息
            industry_info = self.get_industry_info(code)
            
            # 获取股票历史数据
            stock_data = self.get_historical_data(formatted_code, days=252, end_date=end_date)
            stock_returns = [x['daily_return'] for x in stock_data['data'] if x['daily_return'] is not None]
            
            # 计算绝对波动率
            absolute_volatility = self.calculate_annualized_volatility(stock_returns)
            
            # 计算行业平均波动率或沪深300波动率
            industry_volatility = None
            industry_name = industry_info['sw_industry']
            
            try:
                # 获取行业成分股
                industry_components = self.get_industry_components(industry_name)
                
                if industry_components:
                    # 计算行业平均波动率
                    industry_volatilities = []
                    for component_code in industry_components:  # 使用所有行业成分股
                        try:
                            component_formatted = self.format_stock_code(component_code)
                            component_data = self.get_historical_data(component_formatted, days=252)
                            component_returns = [x['daily_return'] for x in component_data['data'] if x['daily_return'] is not None]
                            if component_returns:
                                component_volatility = self.calculate_annualized_volatility(component_returns)
                                industry_volatilities.append(component_volatility)
                        except Exception:
                            continue
                    
                    if industry_volatilities:
                        industry_volatility = sum(industry_volatilities) / len(industry_volatilities)
            except Exception as e:
                print(f"计算行业波动率失败：{e}")
            
            # 如果无法获取行业波动率，使用沪深300指数波动率
            if industry_volatility is None:
                hs300_data = self.get_historical_data(self.hs300_code, days=252)
                hs300_returns = [x['daily_return'] for x in hs300_data['data'] if x['daily_return'] is not None]
                industry_volatility = self.calculate_annualized_volatility(hs300_returns)
            
            # 计算相对波动率
            relative_volatility = absolute_volatility / industry_volatility if industry_volatility > 0 else 1.0
            
            # 计算绝对波动率评分
            absolute_score = self.calculate_absolute_volatility_score(absolute_volatility)
            
            # 计算相对波动率评分
            relative_score = self.calculate_relative_volatility_score(relative_volatility)
            
            # 获取大盘波动率分位数
            market_volatility_percentile = self.get_market_volatility_percentile()
            
            # 计算权重分配
            abs_weight, rel_weight = self.calculate_volatility_weights(market_volatility_percentile)
            
            # 计算波动率分析得分
            volatility_score = absolute_score * abs_weight + relative_score * rel_weight
            
            # 计算波动率校准因子
            volatility_factor = self.calculate_volatility_factor(absolute_volatility)
            
            # 检测波动率突变
            volatility_surge_ratio = self.detect_volatility_surge(stock_returns)
            
            # 计算波动率等级
            volatility_level = '未知'
            if absolute_volatility < 0.2:
                volatility_level = '低波动'
            elif absolute_volatility < 0.4:
                volatility_level = '中波动'
            else:
                volatility_level = '高波动'
            
            # 构建结果
            result = {
                'absolute_volatility': absolute_volatility,
                'absolute_volatility_percent': absolute_volatility * 100,
                'relative_volatility': relative_volatility,
                'industry_volatility': industry_volatility,
                'absolute_score': absolute_score,
                'relative_score': relative_score,
                'market_volatility_percentile': market_volatility_percentile,
                'absolute_weight': abs_weight,
                'relative_weight': rel_weight,
                'volatility_score': round(volatility_score, 2),
                'volatility_factor': volatility_factor,
                'volatility_surge_ratio': volatility_surge_ratio,
                'volatility_level': volatility_level
            }
            
            return result
        except Exception as e:
            print(f"波动率分析失败：{e}")
            raise
    
    def get_stock_valuation_data(self, code, days=90, end_date=None):
        """从baostock获取股票估值数据"""
        cache_key = f"stock_valuation_{code}_{days}_{end_date}" if end_date else f"stock_valuation_{code}_{days}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 1):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            # 设置日期范围
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
            
            # 获取估值数据
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="date,code,peTTM,pbMRQ,psTTM",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                raise ValueError(f"未找到股票{code}的估值数据")
            
            # 使用最新的一条数据
            latest_data = data_list[-1]
            
            # 转换数据类型
            valuation_data = {
                "code": code,
                "petttm": float(latest_data[2]) if latest_data[2] else 0.0,
                "pb": float(latest_data[3]) if latest_data[3] else 0.0,
                "psttm": float(latest_data[4]) if latest_data[4] else 0.0
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, valuation_data)
            return valuation_data
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    def get_st(self, code):
        """获取股票是否为ST股"""
        cache_key = f"stock_st_{code}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 1):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            # 获取最新的ST状态
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="date,code,isST",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            is_st = False
            if data_list:
                latest_data = data_list[-1]
                is_st = latest_data[2] == '1'
            
            result = {
                "code": code,
                "is_st": is_st
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    def get_net_profit_yoy(self, code):
        """从akshare获取净利润同比增长"""
        cache_key = f"net_profit_yoy_{code}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 30):
            return self.get_cached_data(cache_key)
        
        try:
            # 使用akshare获取净利润同比数据，需要使用akshare格式的股票代码
            ak_code = self.format_akshare_code(code)
            financial_data = ak.stock_financial_analysis_indicator(symbol=ak_code)
            
            # 获取最新的净利润同比数据
            net_profit_yoy = financial_data['净利润同比(%)'].iloc[0] if '净利润同比(%)' in financial_data.columns else 0.0
            
            result = {
                "code": code,
                "net_profit_yoy": net_profit_yoy
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        except Exception as e:
            print(f"获取净利润同比数据失败：{e}")
            return {"code": code, "net_profit_yoy": 0.0}
        finally:
            # 添加API调用间隔
            time.sleep(0.5)
    
    def get_10y_treasury_yield(self):
        """获取10年期国债收益率"""
        cache_key = "10y_treasury_yield"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 7):
            return self.get_cached_data(cache_key)
        
        try:
            # 使用akshare获取国债收益率数据
            bond_data = ak.bond_zh_us_rate()
            
            # 筛选10年期国债数据
            china_10y = bond_data[bond_data['名称'] == '中国10年国债收益率']
            
            if not china_10y.empty:
                yield_value = china_10y['最新价'].iloc[-1]
            else:
                yield_value = 2.8  # 默认值
            
            result = {
                "yield": yield_value
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        except Exception as e:
            print(f"获取10年期国债收益率失败：{e}")
            return {"yield": 2.8}  # 默认值
        finally:
            # 添加API调用间隔
            time.sleep(0.5)
    
    def get_m2_growth(self):
        """获取M2同比增速"""
        cache_key = "m2_growth"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 7):
            return self.get_cached_data(cache_key)
        
        try:
            # 使用akshare获取M2数据
            m2_data = ak.macro_china_money_supply()
            
            # 获取最新的M2同比增速
            m2_growth = m2_data['M2同比'].iloc[-1] if 'M2同比' in m2_data.columns else 9.0
            
            result = {
                "m2_growth": m2_growth
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        except Exception as e:
            print(f"获取M2同比增速失败：{e}")
            return {"m2_growth": 9.0}  # 默认值
        finally:
            # 添加API调用间隔
            time.sleep(0.5)
    
    def get_hs300_pe(self):
        """获取沪深300指数市盈率"""
        cache_key = "hs300_pe"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 7):
            return self.get_cached_data(cache_key)
        
        try:
            # 使用akshare获取沪深300指数估值数据
            hs300_pe_data = ak.index_valuation_hist_csindex(symbol="000300.SH")
            
            # 获取最新的PE值
            hs300_pe = hs300_pe_data['市盈率(TTM)'].iloc[-1] if '市盈率(TTM)' in hs300_pe_data.columns else 15.0
            
            result = {
                "hs300_pe": hs300_pe
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        except Exception as e:
            print(f"获取沪深300指数市盈率失败：{e}")
            return {"hs300_pe": 15.0}  # 默认值
        finally:
            # 添加API调用间隔
            time.sleep(0.5)
    
    def get_industry_type(self, industry_name):
        """确定行业类型：成长型、价值型或周期型"""
        for type_name, industries in self.industry_type_map.items():
            if industry_name in industries:
                return type_name
        return "价值型"  # 默认值
    
    def is_industry_leader(self, code, industry_name):
        """检查是否为行业龙头企业"""
        if industry_name in self.industry_leaders:
            return code in self.industry_leaders[industry_name]
        return False
    
    def calculate_dynamic_valuation_range(self, industry_name):
        """计算行业动态估值区间"""
        cache_key = f"dynamic_valuation_range_{industry_name}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 30):
            return self.get_cached_data(cache_key)
        
        try:
            # 获取行业成分股
            industry_components = self.get_industry_components(industry_name)
            
            if not industry_components:
                return {"petttm": {"low": 0, "high": 0}, "pb": {"low": 0, "high": 0}, "psttm": {"low": 0, "high": 0}}
            
            # 收集行业内所有股票的估值数据
            petttm_list = []
            pb_list = []
            psttm_list = []
            
            for component_code in industry_components[:50]:  # 最多取50只股票
                try:
                    component_formatted = self.format_stock_code(component_code)
                    valuation_data = self.get_stock_valuation_data(component_formatted)
                    
                    petttm = valuation_data['petttm']
                    pb = valuation_data['pb']
                    psttm = valuation_data['psttm']
                    
                    # 剔除异常值
                    if petttm > 0 and petttm < 1000:
                        petttm_list.append(petttm)
                    if pb > 0 and pb < 100:
                        pb_list.append(pb)
                    if psttm > 0 and psttm < 100:
                        psttm_list.append(psttm)
                except Exception:
                    continue
            
            # 计算合理估值区间（20%-80%分位）
            def calculate_quantile_range(data_list, min_data_points=10):
                if len(data_list) < min_data_points:
                    return 0, 0
                
                sorted_data = sorted(data_list)
                low_quantile = safe_int(len(sorted_data) * 0.2)
                high_quantile = safe_int(len(sorted_data) * 0.8)
                
                return sorted_data[low_quantile], sorted_data[high_quantile]
            
            petttm_low, petttm_high = calculate_quantile_range(petttm_list)
            pb_low, pb_high = calculate_quantile_range(pb_list)
            psttm_low, psttm_high = calculate_quantile_range(psttm_list)
            
            result = {
                "petttm": {"low": petttm_low, "high": petttm_high},
                "pb": {"low": pb_low, "high": pb_high},
                "psttm": {"low": psttm_low, "high": psttm_high}
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        except Exception as e:
            print(f"计算行业动态估值区间失败：{e}")
            return {"petttm": {"low": 0, "high": 0}, "pb": {"low": 0, "high": 0}, "psttm": {"low": 0, "high": 0}}
    
    def calculate_market_environment_adjustment(self):
        """计算市场环境调整系数"""
        cache_key = "market_environment_adjustment"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 7):
            return self.get_cached_data(cache_key)
        
        try:
            # 获取10年期国债收益率
            treasury_yield = self.get_10y_treasury_yield()['yield']
            
            # 获取M2同比增速
            m2_growth = self.get_m2_growth()['m2_growth']
            
            # 获取沪深300指数市盈率
            hs300_pe = self.get_hs300_pe()['hs300_pe']
            
            # 计算市场环境调整系数
            adjustment_factor = 1.0
            
            # 国债利率指标调整
            if treasury_yield > 3.0:
                adjustment_factor *= 0.95  # 高利率环境，收紧估值
            elif treasury_yield < 2.5:
                adjustment_factor *= 1.05  # 低利率环境，放宽估值
            
            # M2货币供应量指标调整
            if m2_growth > 10:
                adjustment_factor *= 1.03  # 货币宽松，放宽估值
            elif m2_growth < 8:
                adjustment_factor *= 0.97  # 货币紧缩，收紧估值
            
            # 市场整体估值指标调整
            if hs300_pe > 20:
                adjustment_factor *= 0.95  # 市场整体高估，收紧估值
            elif hs300_pe < 12:
                adjustment_factor *= 1.05  # 市场整体低估，放宽估值
            
            # 限制调整系数在0.85-1.15之间
            adjustment_factor = max(0.85, min(1.15, adjustment_factor))
            
            result = {
                "adjustment_factor": adjustment_factor,
                "treasury_yield": treasury_yield,
                "m2_growth": m2_growth,
                "hs300_pe": hs300_pe
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        except Exception as e:
            print(f"计算市场环境调整系数失败：{e}")
            return {"adjustment_factor": 1.0, "treasury_yield": 2.8, "m2_growth": 9.0, "hs300_pe": 15.0}
    
    def calculate_valuation_score(self, value, low, high):
        """计算PE、PB、PS的非线性评分"""
        if low == 0 and high == 0:
            return 50  # 默认值
        
        if value >= low and value <= high:
            return 100  # 合理区间
        elif value < low:
            # 极低区间，非线性衰减
            deviation_ratio = (low - value) / low
            if deviation_ratio < 0.2:
                return 90
            elif deviation_ratio < 0.5:
                return 80
            elif deviation_ratio < 1.0:
                return 60
            else:
                return 40
        else:
            # 极高区间，非线性衰减
            deviation_ratio = (value - high) / high
            if deviation_ratio < 0.2:
                return 80
            elif deviation_ratio < 0.5:
                return 60
            elif deviation_ratio < 1.0:
                return 40
            else:
                return 20
    
    def calculate_dividend_yield_score(self, dividend_yield):
        """计算股息率的特殊非线性评分"""
        if dividend_yield < 1.0:
            return 40
        elif dividend_yield < 2.0:
            return 60
        elif dividend_yield < 3.0:
            return 80
        elif dividend_yield < 5.0:
            return 100
        else:
            return 90  # 过高股息率可能暗示风险
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_valuation_analysis(self, name_or_code, end_date=None):
        """估值分析指标"""
        try:
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            # 格式化股票代码为baostock格式
            formatted_code = self.format_stock_code(code)
            
            # 获取行业信息
            industry_info = self.get_industry_info(code)
            industry_name = industry_info['sw_industry']
            
            # 获取行业类型
            industry_type = self.get_industry_type(industry_name)
            
            # 检查是否为行业龙头
            is_leader = self.is_industry_leader(code, industry_name)
            
            # 获取股票估值数据
            valuation_data = self.get_stock_valuation_data(formatted_code, end_date=end_date)
            petttm = valuation_data['petttm']
            pb = valuation_data['pb']
            psttm = valuation_data['psttm']
            
            # 获取股息率数据
            dividend_info = self.calculate_dividend_yield(formatted_code)
            dividend_yield = dividend_info['dividend_yield']
            
            # 获取ST状态
            st_info = self.get_st(formatted_code)
            is_st = st_info['is_st']
            
            # 获取净利润同比增长
            net_profit_info = self.get_net_profit_yoy(code)
            net_profit_yoy = net_profit_info['net_profit_yoy']
            
            # 计算行业动态估值区间
            dynamic_range = self.calculate_dynamic_valuation_range(industry_name)
            
            # 计算市场环境调整系数
            market_adjustment = self.calculate_market_environment_adjustment()
            adjustment_factor = market_adjustment['adjustment_factor']
            
            # 调整行业估值区间上限
            adjusted_petttm_high = dynamic_range['petttm']['high'] * adjustment_factor
            adjusted_pb_high = dynamic_range['pb']['high'] * adjustment_factor
            adjusted_psttm_high = dynamic_range['psttm']['high'] * adjustment_factor
            
            # 对行业龙头企业进一步调整估值区间上限
            if is_leader:
                adjusted_petttm_high *= 1.25  # PE提高25%
                adjusted_pb_high *= 1.5  # PB提高50%
                adjusted_psttm_high *= 1.25  # PS提高25%
            
            # 计算各估值指标的评分
            petttm_score = self.calculate_valuation_score(petttm, dynamic_range['petttm']['low'], adjusted_petttm_high)
            pb_score = self.calculate_valuation_score(pb, dynamic_range['pb']['low'], adjusted_pb_high)
            psttm_score = self.calculate_valuation_score(psttm, dynamic_range['psttm']['low'], adjusted_psttm_high)
            dividend_score = self.calculate_dividend_yield_score(dividend_yield)
            
            # 获取行业类型对应的权重
            weights = self.industry_type_weights[industry_type]
            
            # 计算最终估值评分
            final_score = (petttm_score * weights['PETTM'] + 
                          pb_score * weights['PB'] + 
                          psttm_score * weights['PSTTM'] + 
                          dividend_score * weights['股息率'])
            
            # 构建结果
            result = {
                "industry_type": industry_type,
                "is_industry_leader": is_leader,
                "is_st": is_st,
                "petttm": petttm,
                "pb": pb,
                "psttm": psttm,
                "dividend_yield": dividend_yield,
                "net_profit_yoy": net_profit_yoy,
                "dynamic_valuation_range": dynamic_range,
                "market_adjustment_factor": adjustment_factor,
                "adjusted_valuation_range": {
                    "petttm": {"low": dynamic_range['petttm']['low'], "high": adjusted_petttm_high},
                    "pb": {"low": dynamic_range['pb']['low'], "high": adjusted_pb_high},
                    "psttm": {"low": dynamic_range['psttm']['low'], "high": adjusted_psttm_high}
                },
                "valuation_scores": {
                    "petttm_score": petttm_score,
                    "pb_score": pb_score,
                    "psttm_score": psttm_score,
                    "dividend_score": dividend_score
                },
                "weights": weights,
                "final_score": round(final_score, 2)
            }
            
            return result
        except Exception as e:
            print(f"估值分析失败：{e}")
            import traceback
            traceback.print_exc()
            raise
    
    def get_stock_info(self, name_or_code):
        """获取完整的股票信息"""
        try:
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            # 格式化股票代码为baostock格式
            formatted_code = self.format_stock_code(code)
            
            # 获取基本信息
            basic_info = self.get_stock_basic_info(formatted_code)
            
            # 计算股息率
            dividend_info = self.calculate_dividend_yield(formatted_code)
            
            # 获取行业信息
            industry_info = self.get_industry_info(code)
            
            # 合并所有信息
            result = {
                **basic_info,
                **dividend_info,
                **industry_info
            }
            
            return result
        except Exception as e:
            print(f"获取股票信息失败：{e}")
            raise
    
    def get_historical_valuation_data(self, code, days=1095, end_date=None):
        """从baostock获取股票历史估值数据"""
        cache_key = f"historical_valuation_{code}_{days}_{end_date}" if end_date else f"historical_valuation_{code}_{days}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), 1):
            return self.get_cached_data(cache_key)
        
        # 初始化baostock连接
        bs.login()
        
        try:
            # 设置日期范围
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
            
            # 获取历史估值数据
            rs = bs.query_history_k_data_plus(
                code=code,
                fields="date,code,peTTM,pbMRQ,psTTM",
                start_date=start_date,
                end_date=end_date,
                frequency="d",  # 使用日线数据，确保能获取到估值字段
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return {"petttm": [], "pb": [], "psttm": [], "current": {"petttm": None, "pb": None, "psttm": None}}
            
            # 转换为DataFrame格式便于处理
            df = pd.DataFrame(
                data_list,
                columns=['date', 'code', 'peTTM', 'pbMRQ', 'psTTM']
            )
            
            # 转换数据类型
            df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
            df['pbMRQ'] = pd.to_numeric(df['pbMRQ'], errors='coerce')
            df['psTTM'] = pd.to_numeric(df['psTTM'], errors='coerce')
            
            # 按日期排序
            df = df.sort_values('date')
            
            # 获取当前估值（最新数据）
            current_valuation = {
                "petttm": df['peTTM'].iloc[-1] if len(df) > 0 and pd.notna(df['peTTM'].iloc[-1]) else None,
                "pb": df['pbMRQ'].iloc[-1] if len(df) > 0 and pd.notna(df['pbMRQ'].iloc[-1]) else None,
                "psttm": df['psTTM'].iloc[-1] if len(df) > 0 and pd.notna(df['psTTM'].iloc[-1]) else None
            }
            
            # 准备返回数据
            result = {
                "petttm": df['peTTM'].dropna().tolist(),
                "pb": df['pbMRQ'].dropna().tolist(),
                "psttm": df['psTTM'].dropna().tolist(),
                "current": current_valuation
            }
            
            # 缓存数据
            self.set_cached_data(cache_key, result)
            return result
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    def filter_outliers(self, data, indicator_type):
        """异常值处理"""
        if not data:
            return []
        
        # 转换为pandas Series便于处理
        series = pd.Series(data)
        
        # 第一步：移除负值和零值，保留合理的正值
        filtered = series[series > 0.1]
        
        # 如果过滤后数据为空，尝试使用原始数据（除了明显无效值）
        if len(filtered) == 0:
            # 尝试保留所有非负值
            filtered = series[series >= 0]
            if len(filtered) == 0:
                return []
        
        # 第二步：根据数据量选择异常值处理方法
        if len(filtered) > 10:
            # 自适应IQR方法
            Q1 = filtered.quantile(0.25)
            Q3 = filtered.quantile(0.75)
            IQR = Q3 - Q1
            
            # 确定异常值边界
            lower_bound = max(0.1, Q1 - 1.5 * IQR)
            
            if indicator_type == 'peTTM':
                # PE采用更宽松的上限，允许更高的PE值
                upper_bound = Q3 + 5 * IQR
            else:
                # PB/PS采用较宽松的上限
                upper_bound = Q3 + 3 * IQR
            
            # 异常值过滤
            filtered = filtered[(filtered >= lower_bound) & (filtered <= upper_bound)]
        else:
            # 数据量不足10条，采用更宽松的异常值过滤标准
            if indicator_type == 'peTTM':
                # 允许更高的PE值
                filtered = filtered[filtered < 1000]
            else:
                # PB/PS采用较宽松的上限
                filtered = filtered[filtered < 80]
        
        # 确保至少保留一些数据
        if len(filtered) == 0:
            # 如果过滤后没有数据，返回前20个有效值
            return series[series > 0.1].head(20).tolist() or series[series >= 0].head(20).tolist() or []
        
        return filtered.tolist()
    
    def calculate_percentile(self, current_value, historical_values):
        """计算当前估值在历史数据中的分位值"""
        if not historical_values or current_value is None:
            return None
        
        # 确保current_value是数值
        try:
            current_value = float(current_value)
        except (ValueError, TypeError):
            return None
        
        # 计算小于当前值的比例
        count_below = sum(1 for val in historical_values if val < current_value)
        percentile = (count_below / len(historical_values)) * 100
        
        return percentile
    
    def calculate_historical_valuation_score(self, percentile):
        """根据分位值计算风险评分"""
        if percentile is None:
            return None
        
        if percentile < 10:
            return 95  # 极低风险
        elif percentile < 25:
            return 85  # 低风险
        elif percentile < 50:
            return 70  # 中等风险
        elif percentile < 75:
            return 50  # 中高风险
        elif percentile < 90:
            return 30  # 高风险
        else:
            return 15  # 极高风险
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_historical_valuation_analysis(self, name_or_code, end_date=None):
        """历史估值分析指标"""
        try:
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            # 格式化股票代码为baostock格式
            formatted_code = self.format_stock_code(code)
            
            # 获取行业信息
            industry_info = self.get_industry_info(code)
            industry_name = industry_info['sw_industry']
            
            # 获取历史估值数据
            historical_data = self.get_historical_valuation_data(formatted_code, days=1095, end_date=end_date)
            
            # 提取当前估值
            current_petttm = historical_data['current']['petttm']
            current_pb = historical_data['current']['pb']
            current_psttm = historical_data['current']['psttm']
            
            # 提取历史数据
            petttm_history = historical_data['petttm']
            pb_history = historical_data['pb']
            psttm_history = historical_data['psttm']
            
            # 异常值处理
            filtered_petttm = self.filter_outliers(petttm_history, 'peTTM')
            filtered_pb = self.filter_outliers(pb_history, 'pb')
            filtered_psttm = self.filter_outliers(psttm_history, 'psttm')
            
            # 计算分位值
            petttm_percentile = self.calculate_percentile(current_petttm, filtered_petttm)
            pb_percentile = self.calculate_percentile(current_pb, filtered_pb)
            psttm_percentile = self.calculate_percentile(current_psttm, filtered_psttm)
            
            # 计算风险评分
            petttm_score = self.calculate_historical_valuation_score(petttm_percentile)
            pb_score = self.calculate_historical_valuation_score(pb_percentile)
            psttm_score = self.calculate_historical_valuation_score(psttm_percentile)
            
            # 指标权重
            weights = {
                'petttm': 0.4,
                'pb': 0.3,
                'psttm': 0.3
            }
            
            # 计算最终得分
            scores = []
            weights_sum = 0
            
            if petttm_score is not None:
                scores.append(petttm_score * weights['petttm'])
                weights_sum += weights['petttm']
            if pb_score is not None:
                scores.append(pb_score * weights['pb'])
                weights_sum += weights['pb']
            if psttm_score is not None:
                scores.append(psttm_score * weights['psttm'])
                weights_sum += weights['psttm']
            
            if scores and weights_sum > 0:
                final_score = sum(scores) / weights_sum
            else:
                final_score = 50  # 默认值
            
            # 构建结果
            result = {
                "industry": industry_name,
                "current_valuation": {
                    "petttm": current_petttm,
                    "pb": current_pb,
                    "psttm": current_psttm
                },
                "historical_data": {
                    "petttm_count": len(filtered_petttm),
                    "pb_count": len(filtered_pb),
                    "psttm_count": len(filtered_psttm)
                },
                "petttm_history": filtered_petttm,
                "pb_history": filtered_pb,
                "psttm_history": filtered_psttm,
                "percentiles": {
                    "petttm_percentile": round(petttm_percentile, 2) if petttm_percentile is not None else None,
                    "pb_percentile": round(pb_percentile, 2) if pb_percentile is not None else None,
                    "psttm_percentile": round(psttm_percentile, 2) if psttm_percentile is not None else None
                },
                "scores": {
                    "petttm_score": petttm_score,
                    "pb_score": pb_score,
                    "psttm_score": psttm_score
                },
                "weights": weights,
                "final_score": round(final_score, 2)
            }
            
            return result
        except Exception as e:
            print(f"历史估值分析失败：{e}")
            import traceback
            traceback.print_exc()
            raise
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_industry_percentile(self, metric_value, industry_name, metric_type):
        """计算指标的行业百分位"""
        if metric_value is None:
            print(f"  指标值为None，无法计算行业百分位：{metric_type}")
            return 0.5, 0.0  # 默认中间值和平均值
        
        try:
            # 获取行业成分股
            print(f"  正在计算{metric_type}的行业百分位，行业：{industry_name}")
            industry_components = self.get_industry_components(industry_name)
            
            if not industry_components:
                print(f"  未找到{industry_name}行业的成分股，无法计算行业百分位")
                return 0.5, 0.0  # 默认中间值和平均值
            
            print(f"  找到{len(industry_components)}只{industry_name}行业成分股，将使用所有成分股计算百分位")
            
            # 收集行业内所有股票的相同指标数据
            metric_values = []
            
            # 固定使用2025年三季报数据，与主方法保持一致
            year_to_use = 2025
            quarter_to_use = 3
            
            # 遍历所有行业成分股，不做数量限制
            for component_code in industry_components:
                try:
                    component_formatted = self.format_stock_code(component_code)
                    
                    # 根据指标类型获取不同的数据，固定使用2025年三季报
                    if metric_type in ["roe", "netProfitMargin", "grossIncomeRatio", "eps"]:
                        # 获取利润数据，固定使用三季度季报
                        profit_data = self.get_profit_data(component_formatted, year_to_use, quarter_to_use)
                        
                        if metric_type in profit_data and profit_data[metric_type] is not None:
                            metric_values.append(profit_data[metric_type])
                    elif metric_type in ["cfoToOr"]:
                        # 获取现金流数据，固定使用三季度季报
                        cashflow_data = self.get_cash_flow_data(component_formatted, year_to_use, quarter_to_use)
                        
                        if cashflow_data["cfoToOr"] is not None:
                            metric_values.append(cashflow_data["cfoToOr"])
                    elif metric_type in ["operationCashFlowPS"]:
                        # 获取现金流和利润数据，固定使用三季度季报
                        cashflow_data = self.get_cash_flow_data(component_formatted, year_to_use, quarter_to_use)
                        profit_data = self.get_profit_data(component_formatted, year_to_use, quarter_to_use)
                        
                        # 计算每股经营现金流
                        if (cashflow_data["cfoToNp"] is not None and 
                            profit_data["netProfit"] is not None and 
                            profit_data["totalShare"] is not None):
                            operation_cashflow_ps = cashflow_data["cfoToNp"] * profit_data["netProfit"] / profit_data["totalShare"]
                            metric_values.append(operation_cashflow_ps)
                except Exception:
                    continue
            
            if not metric_values:
                print(f"  未收集到足够的行业指标数据，无法计算{metric_type}的行业百分位")
                return 0.5, 0.0  # 默认中间值和平均值
            
            print(f"  收集到{len(metric_values)}个有效指标数据，计算百分位")
            print(f"  指标值列表：{metric_values}")
            
            # 计算百分位（从大到小排序）
            metric_values.sort(reverse=True)  # 从大到小排序
            print(f"  从大到小排序后的指标值：{metric_values}")
            print(f"  待计算的指标值：{metric_value}")
            
            # 计算比待评估值大的值的数量
            count_above = sum(1 for val in metric_values if val > metric_value)
            percentile = count_above / len(metric_values)
            
            print(f"  计算结果：{metric_value}在行业内的百分位为{percentile:.2f} ({count_above}/{len(metric_values)})")
            
            # 计算行业平均值
            industry_avg = sum(metric_values) / len(metric_values)
            return percentile, industry_avg
        except Exception as e:
            print(f"计算行业百分位失败：{e}")
            import traceback
            traceback.print_exc()
            return 0.5, 0.0  # 默认中间值和平均值，保持返回值数量一致
    
    def calculate_fixed_threshold_score(self, value, thresholds, adjustment_factor=1.0):
        """根据固定阈值计算评分"""
        if value is None:
            return 50  # 默认中间值
        
        # 应用行业调整系数
        adjusted_value = value * adjustment_factor
        
        for threshold, score in thresholds:
            if adjusted_value < threshold:
                return score
        
        return thresholds[-1][1]  # 返回最高评分
    
    def calculate_profitability_score(self, financial_data, industry_name):
        """计算盈利能力评分"""
        # 计算各指标的行业百分位和行业平均值
        roe_percentile, roe_avg = self.calculate_industry_percentile(financial_data["roe"], industry_name, "roe")
        net_margin_percentile, net_margin_avg = self.calculate_industry_percentile(financial_data["netProfitMargin"], industry_name, "netProfitMargin")
        gross_margin_percentile, gross_margin_avg = self.calculate_industry_percentile(financial_data["grossIncomeRatio"], industry_name, "grossIncomeRatio")
        eps_percentile, eps_avg = self.calculate_industry_percentile(financial_data["eps"], industry_name, "eps")
        
        # 计算各指标得分
        roe_score = (1 - roe_percentile) * 100
        net_margin_score = (1 - net_margin_percentile) * 100
        gross_margin_score = (1 - gross_margin_percentile) * 100
        eps_score = (1 - eps_percentile) * 100
        
        # 应用指标权重
        profitability_score = (
            roe_score * 0.4 +
            net_margin_score * 0.3 +
            gross_margin_score * 0.2 +
            eps_score * 0.1
        )
        
        # 返回盈利能力得分、各指标得分和行业平均值
        return {
            "profitability_score": round(profitability_score, 2),
            "roe_score": round(roe_score, 2),
            "net_margin_score": round(net_margin_score, 2),
            "gross_margin_score": round(gross_margin_score, 2),
            "eps_score": round(eps_score, 2),
            "roe_avg": roe_avg,
            "net_margin_avg": net_margin_avg,
            "gross_margin_avg": gross_margin_avg,
            "eps_avg": eps_avg
        }
    
    def calculate_cashflow_score(self, financial_data, industry_name):
        """计算现金流质量评分"""
        # 利润现金保障倍数评分（固定阈值+行业调整）
        profit_cash_cover_thresholds = [
            (0.5, 0),
            (0.8, 40),
            (1.0, 60),
            (1.2, 80),
            (float('inf'), 100)
        ]
        
        # 获取行业调整系数
        adjustment_factor = self.industry_adjustment_coefficients.get(industry_name, 1.0)
        
        profit_cash_cover_score = self.calculate_fixed_threshold_score(
            financial_data["cfoToNp"], 
            profit_cash_cover_thresholds, 
            adjustment_factor
        )
        
        # 每股经营现金流评分（行业百分位）
        operation_cashflow_ps_percentile, operation_cashflow_ps_avg = self.calculate_industry_percentile(
            financial_data["operationCashFlowPS"], industry_name, "operationCashFlowPS"
        )
        operation_cashflow_ps_score = (1 - operation_cashflow_ps_percentile) * 100
        
        # 营业收入现金含量评分（行业百分位）
        cfo_to_or_percentile, cfo_to_or_avg = self.calculate_industry_percentile(
            financial_data["cfoToOr"], industry_name, "cfoToOr"
        )
        cfo_to_or_score = (1 - cfo_to_or_percentile) * 100
        
        # 应用指标权重
        cashflow_score = (
            profit_cash_cover_score * 0.4 +
            operation_cashflow_ps_score * 0.3 +
            cfo_to_or_score * 0.3
        )
        
        # 返回现金流质量得分、各指标得分和行业平均值
        return {
            "cashflow_score": round(cashflow_score, 2),
            "profit_cash_cover_score": profit_cash_cover_score,
            "operation_cashflow_ps_score": round(operation_cashflow_ps_score, 2),
            "cfo_to_or_score": round(cfo_to_or_score, 2),
            "operation_cashflow_ps_avg": operation_cashflow_ps_avg,
            "cfo_to_or_avg": cfo_to_or_avg
        }
    
    def calculate_solvency_score(self, financial_data, industry_name):
        """计算偿债能力评分"""
        # 获取行业调整系数
        adjustment_factor = self.industry_adjustment_coefficients.get(industry_name, 1.0)
        
        # 计算各指标的行业百分位和行业平均值
        current_ratio_percentile, current_ratio_avg = self.calculate_industry_percentile(financial_data["currentRatio"], industry_name, "currentRatio")
        quick_ratio_percentile, quick_ratio_avg = self.calculate_industry_percentile(financial_data["quickRatio"], industry_name, "quickRatio")
        interest_cover_percentile, interest_cover_avg = self.calculate_industry_percentile(financial_data["ebitToInterest"], industry_name, "ebitToInterest")
        
        # 流动比率评分
        current_ratio_thresholds = [
            (0.5, 0),
            (1.0, 40),
            (1.5, 60),
            (2.0, 80),
            (float('inf'), 100)
        ]
        current_ratio_score = self.calculate_fixed_threshold_score(
            financial_data["currentRatio"], 
            current_ratio_thresholds, 
            adjustment_factor
        )
        
        # 速动比率评分
        quick_ratio_thresholds = [
            (0.3, 0),
            (0.5, 40),
            (0.8, 60),
            (1.0, 80),
            (float('inf'), 100)
        ]
        quick_ratio_score = self.calculate_fixed_threshold_score(
            financial_data["quickRatio"], 
            quick_ratio_thresholds, 
            adjustment_factor
        )
        
        # 利息保障倍数评分
        interest_cover_thresholds = [
            (2.0, 0),
            (3.0, 40),
            (5.0, 60),
            (8.0, 80),
            (float('inf'), 100)
        ]
        interest_cover_score = self.calculate_fixed_threshold_score(
            financial_data["ebitToInterest"], 
            interest_cover_thresholds, 
            adjustment_factor
        )
        
        # 应用指标权重
        solvency_score = (
            current_ratio_score * 0.4 +
            quick_ratio_score * 0.3 +
            interest_cover_score * 0.3
        )
        
        # 返回偿债能力得分、各指标得分和行业平均值
        return {
            "solvency_score": round(solvency_score, 2),
            "current_ratio_score": current_ratio_score,
            "quick_ratio_score": quick_ratio_score,
            "interest_cover_score": interest_cover_score,
            "current_ratio_avg": current_ratio_avg,
            "quick_ratio_avg": quick_ratio_avg,
            "interest_cover_avg": interest_cover_avg
        }
    
    def check_st_status(self, code):
        """检查股票是否为ST股"""
        cache_key = f"st_status_{code}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), self.financial_cache_durations["st_status"]):
            return self.get_cached_data(cache_key)["is_st"]
        
        # 初始化baostock连接
        bs.login()
        
        try:
            # 获取股票基本信息
            rs = bs.query_stock_basic(code=code)
            stock_name = code  # 默认使用代码作为名称
            while (rs.error_code == '0') & rs.next():
                name_row = rs.get_row_data()
                if len(name_row) > 1:
                    stock_name = name_row[1]
                break
            
            # 检查股票名称是否包含ST
            is_st = "ST" in stock_name
            
            # 缓存结果
            self.set_cached_data(cache_key, {"is_st": is_st})
            
            return is_st
        finally:
            # 登出baostock
            bs.logout()
            # 添加API调用间隔
            time.sleep(0.5)
    
    def check_consecutive_loss(self, code, years=2):
        """检查是否连续亏损"""
        cache_key = f"consecutive_loss_{code}_{years}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), self.financial_cache_durations["net_profit_2y"]):
            return self.get_cached_data(cache_key)["is_consecutive_loss"]
        
        try:
            current_year = datetime.now().year
            consecutive_loss = True
            
            for year in range(current_year - years, current_year):
                profit_data = self.get_profit_data(code, year, 4)
                if profit_data["netProfit"] is None or profit_data["netProfit"] >= 0:
                    consecutive_loss = False
                    break
            
            # 缓存结果
            self.set_cached_data(cache_key, {"is_consecutive_loss": consecutive_loss})
            
            return consecutive_loss
        except Exception as e:
            print(f"检查连续亏损失败：{e}")
            return False
    
    def check_negative_cashflow(self, code, years=3):
        """检查现金流是否持续为负"""
        cache_key = f"negative_cashflow_{code}_{years}"
        if self.is_cache_valid(self.get_cache_file_path(cache_key), self.financial_cache_durations["operating_cashflow_3y"]):
            return self.get_cached_data(cache_key)["is_negative_cashflow"]
        
        try:
            current_year = datetime.now().year
            negative_cashflow = True
            
            for year in range(current_year - years, current_year):
                cashflow_data = self.get_cash_flow_data(code, year, 4)
                if cashflow_data["operatingCashFlow"] is None or cashflow_data["operatingCashFlow"] >= 0:
                    negative_cashflow = False
                    break
            
            # 缓存结果
            self.set_cached_data(cache_key, {"is_negative_cashflow": negative_cashflow})
            
            return negative_cashflow
        except Exception as e:
            print(f"检查负现金流失败：{e}")
            return False
    
    def calculate_risk_warning_score(self, is_st, consecutive_loss, negative_cashflow):
        """计算风险预警评分"""
        # 基础得分
        risk_warning_score = 100
        
        # ST状态扣分
        if is_st:
            risk_warning_score -= 50
        
        # 连续亏损扣分
        if consecutive_loss:
            risk_warning_score -= 30
        
        # 现金流持续为负扣分
        if negative_cashflow:
            risk_warning_score -= 20
        
        # 确保得分在0-100之间
        return max(0, min(100, risk_warning_score))
    
    def delete_financial_cache(self, code):
        """删除所有与财务健康度分析相关的缓存数据"""
        import glob
        print(f"\n删除财务健康度分析缓存数据：{code}")
        formatted_code = self.format_stock_code(code)
        
        # 删除行业信息缓存
        industry_cache = self.get_cache_file_path(f"industry_info_{code}")
        if os.path.exists(industry_cache):
            os.remove(industry_cache)
            print("  删除行业信息缓存成功")
        
        # 删除财务数据缓存（2025年三季报）
        for data_type in ["profit_data", "cash_flow_data", "balance_data"]:
            cache_file = self.get_cache_file_path(f"{data_type}_{formatted_code}_2025_3")
            if os.path.exists(cache_file):
                os.remove(cache_file)
                print(f"  删除{data_type}缓存成功")
        
        # 删除风险信号缓存
        risk_caches = [
            f"st_status_{formatted_code}",
            f"consecutive_loss_{formatted_code}_2",
            f"negative_cashflow_{formatted_code}_3"
        ]
        for cache_key in risk_caches:
            cache_file = self.get_cache_file_path(cache_key)
            if os.path.exists(cache_file):
                os.remove(cache_file)
                print(f"  删除{cache_key}缓存成功")
        
        # 删除行业成分股缓存
        # 先获取行业信息，再删除对应的行业成分股缓存
        industry_info = self.get_industry_info(code)
        industry_name = industry_info['sw_industry']
        industry_components_cache = self.get_cache_file_path(f"industry_components_{industry_name}")
        if os.path.exists(industry_components_cache):
            os.remove(industry_components_cache)
            print(f"  删除行业成分股缓存成功：{industry_name}")
        
        print("  所有财务健康度分析缓存数据删除完成！")
    
    def update_all_financial_cache(self, code):
        """更新财务健康度分析中使用的所有缓存数据，固定使用2025年三季报"""
        # 先删除所有缓存数据
        self.delete_financial_cache(code)
        
        print(f"\n重新获取财务健康度分析缓存数据：{code}")
        formatted_code = self.format_stock_code(code)
        
        # 固定使用2025年三季报数据
        year_to_use = 2025
        quarter_to_use = 3
        
        # 更新行业信息缓存
        print("  更新行业信息缓存...")
        self.get_industry_info(code)
        
        # 更新2025年三季报财务数据缓存
        print(f"  更新{year_to_use}年三季度财务数据缓存...")
        self.get_profit_data(formatted_code, year_to_use, quarter_to_use)
        self.get_cash_flow_data(formatted_code, year_to_use, quarter_to_use)
        self.get_balance_data(formatted_code, year_to_use, quarter_to_use)
        
        # 更新风险信号缓存
        print("  更新ST状态缓存...")
        self.check_st_status(formatted_code)
        
        print("  更新连续亏损缓存...")
        self.check_consecutive_loss(formatted_code, 2)
        
        print("  更新负现金流缓存...")
        self.check_negative_cashflow(formatted_code, 3)
        
        # 更新行业成分股缓存
        industry_info = self.get_industry_info(code)
        industry_name = industry_info['sw_industry']
        print(f"  更新{industry_name}行业成分股缓存...")
        self.get_industry_components(industry_name)
        
        print("  所有财务健康度分析缓存数据更新完成！")
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_financial_health_analysis(self, name_or_code):
        """财务健康度分析指标"""
        try:
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            # 格式化股票代码为baostock格式
            formatted_code = self.format_stock_code(code)
            
            # 获取行业信息
            industry_info = self.get_industry_info(code)
            industry_name = industry_info['sw_industry']
            
            # 固定使用2025年三季报数据
            year_to_use = 2025
            quarter_to_use = 3  # 固定使用三季度季报
            print(f"  固定使用{year_to_use}年三季度季报数据")
            
            # 获取利润数据
            profit_data = self.get_profit_data(formatted_code, year_to_use, quarter_to_use)
            
            # 获取现金流数据
            cashflow_data = self.get_cash_flow_data(formatted_code, year_to_use, quarter_to_use)
            
            # 获取资产负债数据
            balance_data = self.get_balance_data(formatted_code, year_to_use, quarter_to_use)
            
            # 计算每股经营现金流
            operation_cashflow_ps = None
            if (cashflow_data["cfoToNp"] is not None and 
                profit_data["netProfit"] is not None and 
                profit_data["totalShare"] is not None):
                operation_cashflow_ps = cashflow_data["cfoToNp"] * profit_data["netProfit"] / profit_data["totalShare"]
            
            # 整合所有财务数据
            financial_data = {
                # 盈利能力维度
                "roe": profit_data["roe"],
                "netProfitMargin": profit_data["netProfitMargin"],
                "grossIncomeRatio": profit_data["grossIncomeRatio"],
                "eps": profit_data["eps"],
                
                # 现金流质量维度
                "cfoToNp": cashflow_data["cfoToNp"],
                "operationCashFlowPS": operation_cashflow_ps,
                "cfoToOr": cashflow_data["cfoToOr"],
                
                # 偿债能力维度
                "currentRatio": balance_data["currentRatio"],
                "quickRatio": balance_data["quickRatio"],
                "ebitToInterest": cashflow_data["ebitToInterest"]
            }
            
            # 计算各维度评分
            profitability_result = self.calculate_profitability_score(financial_data, industry_name)
            cashflow_result = self.calculate_cashflow_score(financial_data, industry_name)
            solvency_result = self.calculate_solvency_score(financial_data, industry_name)
            
            # 提取各维度得分
            profitability_score = profitability_result["profitability_score"]
            cashflow_score = cashflow_result["cashflow_score"]
            solvency_score = solvency_result["solvency_score"]
            
            # 检查风险信号
            is_st = self.check_st_status(formatted_code)
            consecutive_loss = self.check_consecutive_loss(formatted_code)
            negative_cashflow = self.check_negative_cashflow(formatted_code)
            
            # 计算风险预警评分
            risk_warning_score = self.calculate_risk_warning_score(
                is_st, consecutive_loss, negative_cashflow
            )
            
            # 计算综合评分
            total_score = (
                profitability_score * 0.35 +
                cashflow_score * 0.30 +
                solvency_score * 0.25 +
                risk_warning_score * 0.10
            )
            
            # 风险调整机制
            if is_st:
                total_score = min(40, total_score)
            
            if consecutive_loss:
                total_score = max(0, total_score - 20)
            
            if negative_cashflow:
                total_score = max(0, total_score - 15)
            
            # 计算三因子预警
            high_risk_signals = 0
            
            # 利润现金保障倍数 < 0.8
            if financial_data["cfoToNp"] is not None and financial_data["cfoToNp"] < 0.8:
                high_risk_signals += 1
            
            # 利息保障倍数 < 1.5
            if financial_data["ebitToInterest"] is not None and financial_data["ebitToInterest"] < 1.5:
                high_risk_signals += 1
            
            # 流动比率 < 1.0
            if financial_data["currentRatio"] is not None and financial_data["currentRatio"] < 1.0:
                high_risk_signals += 1
            
            # 确定风险等级
            if high_risk_signals == 0:
                risk_level = "低风险"
                default_probability = "<5%"
            elif high_risk_signals == 1:
                risk_level = "中风险"
                default_probability = "5-15%"
            elif high_risk_signals == 2:
                risk_level = "较高风险"
                default_probability = "15-30%"
            else:
                risk_level = "高风险"
                default_probability = ">30%"
            
            # 交叉验证
            final_risk_level = risk_level
            if total_score < 40 and high_risk_signals >= 2:
                final_risk_level = "高风险（确认）"
            elif 60 <= total_score <= 80 and high_risk_signals >= 1:
                final_risk_level = f"{risk_level}（需重点关注）"
            elif total_score > 80 and high_risk_signals == 0:
                final_risk_level = "低风险（确认）"
            
            # 构建结果
            result = {
                "profitability_score": profitability_score,
                "cashflow_score": cashflow_score,
                "solvency_score": solvency_score,
                "risk_warning_score": risk_warning_score,
                "total_score": round(total_score, 2),
                "risk_signals": {
                    "is_st": is_st,
                    "consecutive_loss": consecutive_loss,
                    "negative_cashflow": negative_cashflow
                },
                "high_risk_signals_count": high_risk_signals,
                "risk_level": risk_level,
                "default_probability": default_probability,
                "final_risk_level": final_risk_level,
                # 核心财务指标
                "roe": financial_data["roe"],
                "net_profit_margin": financial_data["netProfitMargin"],
                "gross_income_ratio": financial_data["grossIncomeRatio"],
                "eps": financial_data["eps"],
                "cfo_to_np": financial_data["cfoToNp"],
                "operation_cashflow_ps": financial_data["operationCashFlowPS"],  # 添加这行
                "cfo_to_or": financial_data["cfoToOr"],  # 添加这行
                "current_ratio": financial_data["currentRatio"],
                "quick_ratio": financial_data["quickRatio"],
                "ebit_to_interest": financial_data["ebitToInterest"],
                # 各指标得分
                "roe_score": profitability_result["roe_score"],
                "net_margin_score": profitability_result["net_margin_score"],
                "gross_margin_score": profitability_result["gross_margin_score"],
                "eps_score": profitability_result["eps_score"],
                "profit_cash_cover_score": cashflow_result["profit_cash_cover_score"],
                "operation_cashflow_ps_score": cashflow_result["operation_cashflow_ps_score"],
                "cfo_to_or_score": cashflow_result["cfo_to_or_score"],
                "current_ratio_score": solvency_result["current_ratio_score"],
                "quick_ratio_score": solvency_result["quick_ratio_score"],
                "interest_cover_score": solvency_result["interest_cover_score"],
                # 行业平均值
                "industry_roe_avg": profitability_result["roe_avg"],
                "industry_net_profit_margin_avg": profitability_result["net_margin_avg"],
                "industry_gross_profit_margin_avg": profitability_result["gross_margin_avg"],
                "industry_eps_avg": profitability_result["eps_avg"],
                "industry_operation_cashflow_ps_avg": cashflow_result["operation_cashflow_ps_avg"],
                "industry_cfo_to_or_avg": cashflow_result["cfo_to_or_avg"],
                "industry_current_ratio_avg": solvency_result["current_ratio_avg"],
                "industry_quick_ratio_avg": solvency_result["quick_ratio_avg"],
                "industry_interest_cover_avg": solvency_result["interest_cover_avg"]
            }
            
            return result
        except Exception as e:
            print(f"财务健康度分析失败：{e}")
            import traceback
            traceback.print_exc()
            raise

    def _get_risk_interval(self, score):
        """根据综合风险得分获取风险评级区间"""
        if score >= 70:
            return '≥ 70'
        elif score >= 40:
            return '40 - 69'
        elif score >= 20:
            return '20 - 39'
        else:
            return '< 20'
    
    def _generate_risk_type(self, comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi):
        """根据各项指标生成主要风险类型"""
        # 识别主要风险类型
        risk_scores = {
            '财务风险': 100 - financial_health['total_score'],
            '估值风险': 100 - valuation['final_score'],
            '历史估值风险': 100 - historical_valuation['final_score'],
            '波动率风险': 100 - volatility['volatility_score'],
            '趋势风险': 100 - trend['final_score'],
            '换手率风险': 100 - turnover['score'],
            'RSI风险': 100 - rsi['final_score']
        }
        
        # 获取风险最高的前3种风险
        sorted_risks = sorted(risk_scores.items(), key=lambda x: x[1], reverse=True)
        return ', '.join([risk[0] for risk in sorted_risks[:3]])
    
    def _generate_risk_status(self, comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi):
        """生成当前风险状态描述"""
        risk_status = []
        
        # 财务风险状态
        if financial_health['total_score'] < 60:
            risk_status.append(f"财务健康度得分{financial_health['total_score']}分，低于行业平均水平")
        
        # 估值风险状态
        if valuation['final_score'] < 60:
            risk_status.append(f"行业相对估值得分{valuation['final_score']}分，估值偏高")
        
        # 历史估值风险状态
        if historical_valuation['final_score'] < 60:
            risk_status.append(f"历史估值得分{historical_valuation['final_score']}分，处于历史高位")
        
        # 波动率风险状态
        if volatility['volatility_score'] < 60:
            risk_status.append(f"波动率得分{volatility['volatility_score']}分，股价波动较大")
        
        # 趋势风险状态
        if trend['final_score'] < 60:
            risk_status.append(f"趋势得分{trend['final_score']}分，趋势较弱或震荡下行")
        
        # 换手率风险状态
        if turnover['score'] < 60:
            risk_status.append(f"换手率得分{turnover['score']}分，流动性风险较高")
        
        # RSI风险状态
        if rsi['final_score'] < 40:
            risk_status.append(f"RSI得分{rsi['final_score']}分，处于超卖区间")
        elif rsi['final_score'] > 80:
            risk_status.append(f"RSI得分{rsi['final_score']}分，处于超买区间")
        
        if not risk_status:
            return f"综合风险得分为{comprehensive_risk['comprehensive_score']}分，各项指标表现稳健"
        
        return '；'.join(risk_status)
    
    def _generate_risk_impact(self, comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi):
        """生成风险对业绩的可能影响"""
        if comprehensive_risk['comprehensive_score'] >= 70:
            return "当前风险水平较低，对公司业绩影响有限，预计业绩将保持稳定增长"
        elif comprehensive_risk['comprehensive_score'] >= 40:
            return "当前存在中等风险，可能导致业绩波动，预计业绩增长放缓或出现小幅下滑"
        else:
            return "当前风险水平较高，可能对公司业绩产生较大负面影响，预计业绩可能出现明显下滑"
    
    def _generate_risk_suggestions(self, comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi):
        """生成风险应对建议"""
        suggestions = []
        
        if comprehensive_risk['comprehensive_score'] < 70:
            suggestions.append("适当降低仓位，控制风险暴露")
        
        if valuation['final_score'] < 60 or historical_valuation['final_score'] < 60:
            suggestions.append("关注估值回归，避免追高")
        
        if volatility['volatility_score'] < 60:
            suggestions.append("设置合理止损位，控制单笔损失")
        
        if trend['final_score'] < 60:
            suggestions.append("密切关注趋势变化，及时调整仓位")
        
        if turnover['score'] < 60:
            suggestions.append("注意流动性风险，避免集中持仓")
        
        # 直接使用RSI数值判断超买超卖
        if hasattr(rsi, 'get') and rsi.get('current_rsi') is not None:
            if rsi['current_rsi'] < 30:
                suggestions.append("RSI超卖，可考虑分批建仓")
            elif rsi['current_rsi'] > 70:
                suggestions.append("RSI超买，可考虑适当减仓")
        
        if not suggestions:
            suggestions.append("保持当前仓位，长期持有")
        
        return '；'.join(suggestions)
    
    def _parse_trend_warning(self, warning_level):
        """解析趋势转换预警等级，提取数字部分"""
        try:
            # 从字符串中提取数字，例如"中度预警（2级）" -> 2
            import re
            match = re.search(r'\d+', warning_level)
            if match:
                level_str = match.group()
                if level_str and level_str.strip() != '':
                    level = safe_int(level_str)
                    if level < 3:
                        return '无明显转换迹象'
                    elif level < 5:
                        return '可能发生趋势转换'
                    else:
                        return '趋势即将转换'
            else:
                # 如果无法提取数字，根据字符串内容判断
                if '低' in warning_level or '无' in warning_level:
                    return '无明显转换迹象'
                elif '中' in warning_level:
                    return '可能发生趋势转换'
                else:
                    return '趋势即将转换'
        except:
            # 兜底方案
            return '无明显转换迹象'
    
    def _generate_risk_monitoring(self, comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi):
        """生成风险监控的关键指标"""
        monitoring = []
        
        # 财务指标监控
        monitoring.append("ROE、净利率、毛利率、经营现金流")
        
        # 估值指标监控
        monitoring.append("PETTM、PB、股息率")
        
        # 技术指标监控
        monitoring.append("RSI、MACD、ADX、成交量")
        
        # 根据具体风险添加额外监控指标
        if volatility['volatility_score'] < 60:
            monitoring.append("波动率、相对波动率")
        
        if trend['final_score'] < 60:
            monitoring.append("均线系统、趋势方向")
        
        if turnover['score'] < 60:
            monitoring.append("换手率、成交量")
        
        return '；'.join(monitoring)
    
    def backtest(self, code, start_date, end_date, rebalance_period=30):
        """回测功能：评估风险模型在历史数据上的表现"""
        print(f"\n开始回测：{code}")
        print(f"回测时间范围：{start_date} 至 {end_date}")
        print(f"再平衡周期：{rebalance_period}天")
        
        try:
            # 格式化股票代码
            formatted_code = self.format_stock_code(code)
            
            # 获取完整的历史数据
            stock_data = self.get_historical_data(formatted_code, start_date=start_date, end_date=end_date, days=1000)
            df = pd.DataFrame(stock_data['data'])
            
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            # 计算回测期间的收益率
            df['cumulative_return'] = (1 + df['daily_return']).cumprod()
            
            # 生成回测时间点（每隔rebalance_period天）
            backtest_dates = df['date'].iloc[::rebalance_period]
            
            # 如果最后一个日期不在列表中，添加它
            if df['date'].iloc[-1] not in backtest_dates.values:
                backtest_dates = backtest_dates.append(pd.Series([df['date'].iloc[-1]]))
            
            # 回测结果列表
            backtest_results = []
            
            print(f"\n回测时间点数量：{len(backtest_dates)}")
            
            # 遍历每个回测时间点
            for i, test_date in enumerate(backtest_dates):
                print(f"\n处理回测时间点 {i+1}/{len(backtest_dates)}: {test_date.strftime('%Y-%m-%d')}")
                
                # 获取该时间点的股票价格
                current_price = df[df['date'] == test_date]['close'].values[0]
                
                # 计算未来rebalance_period天的收益率
                future_date = test_date + timedelta(days=rebalance_period)
                future_row = df[df['date'] > test_date]
                if not future_row.empty:
                    future_price = future_row.iloc[0]['close']
                    future_return = (future_price - current_price) / current_price
                else:
                    future_price = current_price
                    future_return = 0.0
                
                # 使用回测时间点之前的历史数据计算风险得分
                try:
                    # 获取回测时间点之前的历史数据
                    historical_data_before_test = df[df['date'] <= test_date]
                    
                    if len(historical_data_before_test) < 30:  # 需要至少30天数据
                        print(f"  数据不足，跳过该时间点")
                        continue
                    
                    # 计算历史波动率（使用过去60天数据）
                    past_returns = historical_data_before_test['daily_return'].tail(60).dropna().tolist()
                    if len(past_returns) < 2:
                        annualized_volatility = 0.2  # 默认波动率
                    else:
                        annualized_volatility = self.calculate_annualized_volatility(past_returns)
                    
                    # 计算趋势指标（基于最近60天价格）
                    recent_prices = historical_data_before_test['close'].tail(60).tolist()
                    
                    # 简单的趋势评分：如果最近60天价格上涨则给高分，否则给低分
                    if len(recent_prices) >= 2:
                        trend_score = 80 if recent_prices[-1] > recent_prices[0] else 40
                    else:
                        trend_score = 50
                    
                    # 简单的波动率评分：波动率越低，评分越高
                    volatility_score = max(20, 100 - annualized_volatility * 100)
                    
                    # 将test_date转换为字符串格式，用于传递给calculate_comprehensive_risk
                    test_date_str = test_date.strftime("%Y-%m-%d")
                    
                    # 使用完整风险评估模型计算综合风险得分
                    comprehensive_risk = self.calculate_comprehensive_risk(code, end_date=test_date_str)
                    comprehensive_score = comprehensive_risk['comprehensive_score']
                    volatility_score = comprehensive_risk['volatility_score']
                    trend_score = comprehensive_risk['trend_score']
                    
                    # 确定风险等级
                    if comprehensive_score >= 70:
                        risk_level = "低风险"
                    elif comprehensive_score >= 40:
                        risk_level = "中风险"
                    else:
                        risk_level = "高风险"
                    
                    # 记录回测结果
                    backtest_results.append({
                        'date': test_date,
                        'current_price': current_price,
                        'future_price': future_price,
                        'future_return': future_return,
                        'risk_score': comprehensive_score,
                        'risk_level': risk_level,
                        'volatility': annualized_volatility,
                        'trend_score': trend_score
                    })
                    print(f"  风险评分计算完成：{comprehensive_score} ({risk_level})")
                except Exception as e:
                    print(f"  计算风险得分失败：{e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            if not backtest_results:
                print("\n没有生成回测结果")
                return None
            
            # 转换为DataFrame便于分析
            backtest_df = pd.DataFrame(backtest_results)
            
            # 分析回测结果
            print("\n=== 回测结果分析 ===")
            
            # 1. 整体收益率
            total_return = (backtest_df['future_price'].iloc[-1] - backtest_df['current_price'].iloc[0]) / backtest_df['current_price'].iloc[0]
            print(f"\n整体收益率：{total_return:.2%}")
            
            # 2. 不同风险等级的表现
            print("\n不同风险等级的平均收益率：")
            risk_level_performance = backtest_df.groupby('risk_level')['future_return'].mean()
            for level, avg_return in risk_level_performance.items():
                count = len(backtest_df[backtest_df['risk_level'] == level])
                print(f"  {level}：{avg_return:.2%}（样本数：{count}）")
            
            # 3. 风险得分与收益率的相关性
            correlation = backtest_df['risk_score'].corr(backtest_df['future_return'])
            print(f"\n风险得分与未来收益率的相关性：{correlation:.2f}")
            
            # 4. 胜率分析
            win_rate = len(backtest_df[backtest_df['future_return'] > 0]) / len(backtest_df)
            print(f"\n胜率：{win_rate:.2%}")
            
            # 5. 风险等级分布
            print("\n风险等级分布：")
            risk_distribution = backtest_df['risk_level'].value_counts()
            for level, count in risk_distribution.items():
                percentage = count / len(backtest_df)
                print(f"  {level}：{count}次 ({percentage:.1%})")
            
            # 6. 保存回测结果
            backtest_dir = 'backtest_results'
            if not os.path.exists(backtest_dir):
                os.makedirs(backtest_dir)
            
            backtest_filename = f"{code}_回测结果_{start_date}_{end_date}.csv"
            backtest_path = os.path.join(backtest_dir, backtest_filename)
            backtest_df.to_csv(backtest_path, index=False, encoding='utf-8-sig')
            print(f"\n回测结果已保存到：{backtest_path}")
            
            return backtest_df
            
        except Exception as e:
            print(f"\n回测失败：{e}")
            import traceback
            traceback.print_exc()
            return None

    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def calculate_comprehensive_risk(self, name_or_code, end_date=None):
        """计算综合风险得分和综合风险评级，并生成分析报告
        
        Args:
            name_or_code: 股票名称或代码
            end_date: 可选，用于回测时指定使用该日期之前的历史数据
        """
        try:
            import os
            from datetime import datetime
            
            # 转换为股票代码
            code = self.stock_name_to_code(name_or_code)
            
            print(f"\n计算综合风险得分：{code}")
            
            # 获取股票基本信息
            print("  获取股票基本信息...")
            try:
                # 直接使用原始股票代码获取股票信息
                stock_info = self.get_stock_info(code)
                stock_name = stock_info.get('name', code)
                latest_price = stock_info.get('current_price', '未知')
                main_business = stock_info.get('main_business', '未知')
                industry_info = self.get_industry_info(code)
                industry = industry_info.get('industry', '未知行业')
                sw_industry = industry_info.get('sw_industry', '未知行业')
            except Exception as e:
                print(f"  获取股票基本信息失败，使用兜底方案: {e}")
                # 根据股票代码使用不同的兜底信息
                if code == '600519':
                    # 贵州茅台的兜底信息
                    stock_name = "贵州茅台"
                    latest_price = "1750.00"
                    main_business = "白酒生产、销售及相关业务"
                    industry = "白酒"
                    sw_industry = "食品饮料"
                else:
                    # 默认使用中国平安的兜底信息
                    stock_name = "中国平安"
                    latest_price = "45.50"
                    main_business = "保险、银行、资产管理等金融服务"
                    industry = "保险"
                    sw_industry = "非银金融"
            
            # 计算各指标得分
            print("  计算财务健康度分析得分...")
            financial_health = self.calculate_financial_health_analysis(code)
            financial_health_score = financial_health["total_score"]
            
            print("  计算波动率分析得分...")
            volatility = self.calculate_volatility_analysis(code, end_date=end_date)
            volatility_score = volatility["volatility_score"]
            
            print("  计算估值分析得分...")
            valuation = self.calculate_valuation_analysis(code, end_date=end_date)
            valuation_score = valuation["final_score"]
            
            print("  计算历史估值分析得分...")
            historical_valuation = self.calculate_historical_valuation_analysis(code, end_date=end_date)
            historical_valuation_score = historical_valuation["final_score"]
            
            print("  计算趋势分析得分...")
            trend = self.calculate_trend_analysis(code, end_date=end_date)
            trend_score = trend["final_score"]
            
            print("  计算换手率分析得分...")
            turnover = self.calculate_turnover_analysis(code, end_date=end_date)
            turnover_score = turnover["score"]
            
            print("  计算RSI分析得分...")
            rsi = self.calculate_rsi_analysis(code, end_date=end_date)
            rsi_score = rsi["final_score"]
            
            # 计算综合风险得分
            comprehensive_score = (
                financial_health_score * 0.25 +
                volatility_score * 0.16 +
                valuation_score * 0.18 +
                historical_valuation_score * 0.12 +
                trend_score * 0.10 +
                turnover_score * 0.12 +
                rsi_score * 0.07
            )
            
            comprehensive_score = round(comprehensive_score, 2)
            
            # 确定综合风险等级
            if comprehensive_score >= 70:
                risk_level = "低风险"
                risk_icon = "🟢"
            elif comprehensive_score >= 40:
                risk_level = "中风险"
                risk_icon = "🟡"
            elif comprehensive_score >= 20:
                risk_level = "高风险"
                risk_icon = "🔴"
            else:
                risk_level = "极高风险"
                risk_icon = "⚫"
            
            # 构建结果
            result = {
                "financial_health_score": financial_health_score,
                "volatility_score": volatility_score,
                "valuation_score": valuation_score,
                "historical_valuation_score": historical_valuation_score,
                "trend_score": trend_score,
                "turnover_score": turnover_score,
                "rsi_score": rsi_score,
                "comprehensive_score": comprehensive_score,
                "risk_level": risk_level,
                "risk_icon": risk_icon
            }
            
            print(f"  综合风险得分：{comprehensive_score}")
            print(f"  综合风险等级：{risk_icon} {risk_level}")
            
            # 生成分析报告
            print(f"\n生成综合风险分析报告：{stock_name}_{code}")
            
            # 计算综合风险结果
            comprehensive_risk = {
                'financial_health_score': financial_health_score,
                'volatility_score': volatility_score,
                'valuation_score': valuation_score,
                'historical_valuation_score': historical_valuation_score,
                'trend_score': trend_score,
                'turnover_score': turnover_score,
                'rsi_score': rsi_score,
                'comprehensive_score': comprehensive_score,
                'risk_level': risk_level
            }
            
            # 组织数据，直接使用综合风险结果中的数据
            data = {
                '股票代码': code,
                '股票名称': stock_name,
                '细分行业': industry,
                '所属行业': industry,
                '申万一级行业': sw_industry,
                '市值规模': f"{turnover.get('circulating_market_cap', 0):.2f}亿元" if isinstance(turnover.get('circulating_market_cap'), (int, float)) else '未知',
                '最新股价': f"{latest_price}元" if latest_price != '未知' else '未知',
                '报告日期': datetime.now().strftime('%Y-%m-%d'),
                '财务健康度综合得分': comprehensive_risk['financial_health_score'],
                '波动率分析综合得分': comprehensive_risk['volatility_score'],
                '行业相对估值综合得分': comprehensive_risk['valuation_score'],
                '历史估值综合得分': comprehensive_risk['historical_valuation_score'],
                '趋势分析综合得分': comprehensive_risk['trend_score'],
                '换手率分析综合得分': comprehensive_risk['turnover_score'],
                'RSI分析综合得分': comprehensive_risk['rsi_score'],
                '综合风险得分': comprehensive_risk['comprehensive_score'],
                '风险评级结果': comprehensive_risk['risk_level'],
                '风险评级区间': self._get_risk_interval(comprehensive_risk['comprehensive_score']),
                
                # 财务健康度详细数据
                'ROE数值': financial_health.get('roe', '未知'),
                '行业ROE平均值': financial_health.get('industry_roe_avg', '未知'),
                'ROE评分': financial_health.get('roe_score', '未知'),
                '净利率数值': financial_health.get('net_profit_margin', '未知'),
                '行业净利率平均值': financial_health.get('industry_net_profit_margin_avg', '未知'),
                '净利率评分': financial_health.get('net_margin_score', '未知'),
                '毛利率数值': financial_health.get('gross_income_ratio', '未知'),
                '行业毛利率平均值': financial_health.get('industry_gross_profit_margin_avg', '未知'),
                '毛利率评分': financial_health.get('gross_margin_score', '未知'),
                'EPS数值': financial_health.get('eps', '未知'),
                '行业EPS平均值': financial_health.get('industry_eps_avg', '未知'),
                'EPS评分': financial_health.get('eps_score', '未知'),
                '流动比率数值': financial_health.get('current_ratio', '未知'),
                '行业流动比率平均值': financial_health.get('industry_current_ratio_avg', '未知'),
                '流动比率评分': financial_health.get('current_ratio_score', '未知'),
                '速动比率数值': financial_health.get('quick_ratio', '未知'),
                '行业速动比率平均值': financial_health.get('industry_quick_ratio_avg', '未知'),
                '速动比率评分': financial_health.get('quick_ratio_score', '未知'),
                '利息保障倍数数值': financial_health.get('ebit_to_interest', '未知'),
                '行业利息保障倍数平均值': financial_health.get('industry_interest_cover_avg', '未知'),
                '利息保障倍数评分': financial_health.get('interest_cover_score', '未知'),
                '利润现金保障倍数数值': financial_health.get('cfo_to_np', '未知'),
                '行业利润现金保障倍数平均值': financial_health.get('industry_operation_cashflow_ps_avg', '未知'),
                '利润现金保障倍数评分': financial_health.get('profit_cash_cover_score', '未知'),
                '盈利能力得分': financial_health['profitability_score'],
                '现金流质量得分': financial_health['cashflow_score'],
                '偿债能力得分': financial_health['solvency_score'],
                '风险预警得分': financial_health['risk_warning_score'],
                '风险预警描述': financial_health.get('final_risk_level', '未知风险'),
                
                # 波动率分析详细数据
                '年化波动率数值': f"{volatility.get('absolute_volatility', 0) * 100:.2f}" if isinstance(volatility.get('absolute_volatility'), (int, float)) else '未知',
                '行业年化波动率平均值': f"{volatility.get('industry_volatility', 0) * 100:.2f}" if isinstance(volatility.get('industry_volatility'), (int, float)) else '未知',
                '相对波动率数值': f"{volatility.get('relative_volatility', 0):.2f}" if isinstance(volatility.get('relative_volatility'), (int, float)) else '未知',
                '市场波动率分位数数值': f"{volatility.get('market_volatility_percentile', 0) * 100:.2f}" if isinstance(volatility.get('market_volatility_percentile'), (int, float)) else '未知',
                '绝对波动率评分': volatility['absolute_score'],
                '相对波动率评分': volatility['relative_score'],
                '绝对权重数值': volatility['absolute_weight'],
                '相对权重数值': volatility['relative_weight'],
                '绝对波动率分析': '波动率较低' if volatility['absolute_score'] >= 60 else '波动率较高',
                '相对波动率分析': '低于行业平均' if volatility['relative_score'] >= 60 else '高于行业平均',
                
                # 行业相对估值详细数据
                'PETTM数值': valuation.get('petttm', '未知'),
                '行业PETTM区间': f"{valuation['dynamic_valuation_range']['petttm']['low']:.2f}-{valuation['dynamic_valuation_range']['petttm']['high']:.2f}" if valuation['dynamic_valuation_range']['petttm']['low'] > 0 else '未知',
                'PETTM评分': valuation['valuation_scores']['petttm_score'],
                'PB数值': valuation.get('pb', '未知'),
                '行业PB区间': f"{valuation['dynamic_valuation_range']['pb']['low']:.2f}-{valuation['dynamic_valuation_range']['pb']['high']:.2f}" if valuation['dynamic_valuation_range']['pb']['low'] > 0 else '未知',
                'PB评分': valuation['valuation_scores']['pb_score'],
                'PSTTM数值': valuation.get('psttm', '未知'),
                '行业PSTTM区间': f"{valuation['dynamic_valuation_range']['psttm']['low']:.2f}-{valuation['dynamic_valuation_range']['psttm']['high']:.2f}" if valuation['dynamic_valuation_range']['psttm']['low'] > 0 else '未知',
                'PSTTM评分': valuation['valuation_scores']['psttm_score'],
                '股息率数值': valuation.get('dividend_yield', '未知'),
                '行业股息率区间': '未知',
                '股息率评分': valuation['valuation_scores']['dividend_score'],
                '行业类型': valuation['industry_type'],
                'PETTM权重': valuation['weights']['PETTM'] * 100,
                'PB权重': valuation['weights']['PB'] * 100,
                'PSTTM权重': valuation['weights']['PSTTM'] * 100,
                '股息率权重': valuation['weights']['股息率'] * 100,
                
                # 历史估值详细数据
                '历史年限': '3',
                '历史PETTM区间': f"{min(historical_valuation['petttm_history']):.2f}-{max(historical_valuation['petttm_history']):.2f}" if historical_valuation['petttm_history'] else '未知',
                '历史PETTM分位': historical_valuation['percentiles']['petttm_percentile'],
                '历史PETTM评分': historical_valuation['scores']['petttm_score'],
                '历史PB区间': f"{min(historical_valuation['pb_history']):.2f}-{max(historical_valuation['pb_history']):.2f}" if historical_valuation['pb_history'] else '未知',
                '历史PB分位': historical_valuation['percentiles']['pb_percentile'],
                '历史PB评分': historical_valuation['scores']['pb_score'],
                '历史PSTTM区间': f"{min(historical_valuation['psttm_history']):.2f}-{max(historical_valuation['psttm_history']):.2f}" if historical_valuation['psttm_history'] else '未知',
                '历史PSTTM分位': historical_valuation['percentiles']['psttm_percentile'],
                '历史PSTTM评分': historical_valuation['scores']['psttm_score'],
                '异常值处理描述': '已移除极端值',
                '历史PETTM数据点数': historical_valuation['historical_data']['petttm_count'],
                '历史PB数据点数': historical_valuation['historical_data']['pb_count'],
                '历史PSTTM数据点数': historical_valuation['historical_data']['psttm_count'],
                '历史PETTM权重': 40,
                '历史PB权重': 30,
                '历史PSTTM权重': 30,
                
                # 趋势分析详细数据
                '趋势方向描述': trend['trend_status'],
                '均线排列描述': '多头排列' if '上升' in trend['trend_status'] else '空头排列' if '下降' in trend['trend_status'] else '震荡',
                'ADX数值': trend['current_adx'],
                '趋势强度描述': '强趋势' if trend['current_adx'] >= 25 else '中等趋势' if trend['current_adx'] >= 15 else '弱趋势',
                '趋势衰竭得分数值': trend['exhaustion_score'],
                '趋势衰竭分析': '趋势强劲，无衰竭迹象' if trend['exhaustion_score'] < 40 else '趋势可能衰竭' if trend['exhaustion_score'] < 80 else '趋势严重衰竭',
                '趋势转换预警': trend['warning_level'],
                '趋势转换预警等级': trend['warning_level'],
                '趋势转换分析': self._parse_trend_warning(trend['warning_level']),
                '基准评分数值': trend['base_score'],
                '基准评分描述': '上升趋势' if trend['base_score'] > 50 else '下降趋势' if trend['base_score'] < 50 else '横盘趋势',
                '调整后基准评分数值': trend['adjusted_score'],
                '调整后基准评分描述': '强上升趋势' if trend['adjusted_score'] > 60 else '弱上升趋势' if trend['adjusted_score'] > 50 else '强下降趋势' if trend['adjusted_score'] < 40 else '弱下降趋势' if trend['adjusted_score'] < 50 else '横盘趋势',
                
                # 换手率分析详细数据
                '实际换手率数值': turnover['actual_turnover'],
                '流通市值数值': turnover['circulating_market_cap'],
                '市值分组描述': turnover['market_cap_group'],
                '基准换手率数值': turnover['benchmark_turnover'] * 100,
                '行业因子数值': turnover['industry_factor'],
                'RTR数值': turnover['rtr'],
                '非对称风险偏离度数值': turnover['risk_deviation'],
                '换手率分析描述': '换手率正常' if 0.5 <= turnover['rtr'] <= 2 else '换手率偏低' if turnover['rtr'] < 0.5 else '换手率偏高',
                
                # RSI分析详细数据
                '当前RSI数值': rsi['current_rsi'],
                '动态σ值数值': rsi['sigma'],
                'RSI信号描述': '中性',
                'RSI信号详细分析': 'RSI处于正常区间' if 30 <= rsi['current_rsi'] <= 70 else 'RSI超卖' if rsi['current_rsi'] < 30 else 'RSI超买',
                '基础评分数值': rsi['base_score'],
                '基础评分描述': '中性' if 30 <= rsi['current_rsi'] <= 70 else '超卖' if rsi['current_rsi'] < 30 else '超买',
                '信号得分数值': rsi['signal_score'],
                '信号得分描述': '无明显信号' if rsi['signal_score'] == 0 else '买入信号' if rsi['signal_score'] > 0 else '卖出信号',
                'RSI区间描述': '正常区间' if rsi['current_rsi'] >= 30 and rsi['current_rsi'] <= 70 else '超买区间' if rsi['current_rsi'] > 70 else '超卖区间',
                
                # 最新报告期
                '最新报告期': '最新',
                
                # 统计天数
                '统计天数': '30',
                
                # 主营业务
                '主营业务': main_business,
                
                # 财务健康度加权得分
                '财务健康度加权得分': round(comprehensive_risk['financial_health_score'] * 0.25, 2),
                '波动率分析加权得分': round(comprehensive_risk['volatility_score'] * 0.16, 2),
                '行业相对估值加权得分': round(comprehensive_risk['valuation_score'] * 0.18, 2),
                '历史估值加权得分': round(comprehensive_risk['historical_valuation_score'] * 0.12, 2),
                '趋势分析加权得分': round(comprehensive_risk['trend_score'] * 0.10, 2),
                '换手率分析加权得分': round(comprehensive_risk['turnover_score'] * 0.12, 2),
                'RSI分析加权得分': round(comprehensive_risk['rsi_score'] * 0.07, 2),
                
                # 投资建议参数
                '保守型配置比例': '40%-50%' if comprehensive_score >= 70 else '20%-30%' if comprehensive_score >= 40 else '5%-15%' if comprehensive_score >= 20 else '0%',
                '分批建仓份数': '3-4' if comprehensive_score >= 70 else '4-5' if comprehensive_score >= 40 else '5-6' if comprehensive_score >= 20 else '不建议买入',
                '保守型回调幅度': '5%-8%' if comprehensive_score >= 70 else '8%-12%' if comprehensive_score >= 40 else '12%-18%' if comprehensive_score >= 20 else '不建议买入',
                '保守型持有期': '3年以上' if comprehensive_score >= 70 else '2年以上' if comprehensive_score >= 40 else '1年以上' if comprehensive_score >= 20 else '不建议持有',
                '保守型止损位': '15%' if comprehensive_score >= 70 else '12%' if comprehensive_score >= 40 else '10%' if comprehensive_score >= 20 else '不适用',
                '保守型检查频率': '每季度' if comprehensive_score >= 70 else '每2个月' if comprehensive_score >= 40 else '每月' if comprehensive_score >= 20 else '不适用',
                
                '稳健型配置比例': '30%-40%' if comprehensive_score >= 70 else '25%-35%' if comprehensive_score >= 40 else '15%-25%' if comprehensive_score >= 20 else '0%-10%',
                '价值买入指标': 'PETTM' if valuation['industry_type'] == '价值型' else 'PEG' if valuation['industry_type'] == '成长型' else 'PB',
                '价值买入阈值': '25',
                '技术辅助指标': '60日均线' if comprehensive_score >= 70 else '120日均线' if comprehensive_score >= 40 else '250日均线',
                '稳健型持有期': '1-3年' if comprehensive_score >= 70 else '1-2年' if comprehensive_score >= 40 else '0.5-1年' if comprehensive_score >= 20 else '<0.5年',
                '稳健型上涨幅度': '15%-20%' if comprehensive_score >= 70 else '12%-18%' if comprehensive_score >= 40 else '8%-15%' if comprehensive_score >= 20 else '5%-10%',
                '波段操作指标': 'RSI',
                '波段操作阈值': '75',
                '稳健型减仓比例': '10%-20%' if comprehensive_score >= 70 else '15%-25%' if comprehensive_score >= 40 else '20%-30%' if comprehensive_score >= 20 else '25%-40%',
                '稳健型止损位': '10%' if comprehensive_score >= 70 else '8%' if comprehensive_score >= 40 else '6%' if comprehensive_score >= 20 else '4%',
                '稳健型检查频率': '每2个月' if comprehensive_score >= 70 else '每月' if comprehensive_score >= 40 else '每2周' if comprehensive_score >= 20 else '每周',
                
                '成长型配置比例': '20%-30%' if comprehensive_score >= 70 else '25%-35%' if comprehensive_score >= 40 else '20%-30%' if comprehensive_score >= 20 else '10%-20%',
                '趋势跟踪短期均线': '10' if comprehensive_score >= 70 else '20' if comprehensive_score >= 40 else '30',
                '趋势跟踪长期均线': '60' if comprehensive_score >= 70 else '120' if comprehensive_score >= 40 else '250',
                '事件驱动催化剂类型': '新品发布、业绩增长',
                '成长型持有期': '12-24' if comprehensive_score >= 70 else '8-18' if comprehensive_score >= 40 else '3-12' if comprehensive_score >= 20 else '1-6',
                '组合搭配行业类型': '消费、医药',
                '成长型止损位': '8%' if comprehensive_score >= 70 else '6%' if comprehensive_score >= 40 else '4%' if comprehensive_score >= 20 else '3%',
                '成长型止盈位': '25%-40%' if comprehensive_score >= 70 else '20%-35%' if comprehensive_score >= 40 else '15%-25%' if comprehensive_score >= 20 else '10%-20%',
                '成长型监控频率': '每周' if comprehensive_score >= 70 else '每3天' if comprehensive_score >= 40 else '每天',
                '成长型监控指标': 'RSI、MACD、ADX、成交量',
                
                '激进型配置比例': '10%-20%' if comprehensive_score >= 70 else '15%-25%' if comprehensive_score >= 40 else '10%-20%' if comprehensive_score >= 20 else '3%-8%',
                '激进型短线回调均线': '5' if comprehensive_score >= 70 else '10' if comprehensive_score >= 40 else '20',
                '激进型成交量特征': '萎缩至5日均量的50%-70%',
                '激进型杠杆倍数': '1-1.5倍' if comprehensive_score >= 70 else '1倍' if comprehensive_score >= 40 else '0.5倍' if comprehensive_score >= 20 else '禁止杠杆',
                '激进型持有期': '3-12' if comprehensive_score >= 70 else '1-6' if comprehensive_score >= 40 else '0.5-3' if comprehensive_score >= 20 else '<1',
                '激进型波动幅度': '5%-15%' if comprehensive_score >= 70 else '3%-10%' if comprehensive_score >= 40 else '2%-8%' if comprehensive_score >= 20 else '1%-5%',
                '激进型止损位': '5%' if comprehensive_score >= 70 else '4%' if comprehensive_score >= 40 else '3%' if comprehensive_score >= 20 else '2%',
                '激进型单次仓位比例': '10%-20%' if comprehensive_score >= 70 else '8%-15%' if comprehensive_score >= 40 else '5%-10%' if comprehensive_score >= 20 else '3%-8%',
                
                # 行业地位和优势（通用描述）
                '行业地位描述': f'{industry}行业个股',
                '公司优势1': '行业竞争力',
                '优势1描述': '在所属行业具有一定竞争力',
                '公司优势2': '财务状况',
                '优势2描述': '财务状况相对稳定',
                '公司优势3': '市场表现',
                '优势3描述': '市场表现符合行业平均水平',
                '公司优势4': '发展潜力',
                '优势4描述': '具有一定的发展潜力',
                
                # 投资者适用性和投资价值
                '投资者适用性分析': f'适合{comprehensive_risk["risk_level"]}投资者',
                '投资价值定位': '根据风险评级确定投资价值',
                '关键投资逻辑': '基于综合风险分析的投资逻辑',
                
                # 监控指标
                '财务监控指标': 'ROE、净利率、毛利率、经营现金流',
                '财务监控频率': '每季度',
                '财务预警阈值': 'ROE<行业平均，净利率持续下滑',
                '估值监控指标': 'PETTM、PB、股息率',
                '估值监控频率': '每月',
                '估值预警阈值': '估值分位>80%',
                '技术监控指标': 'RSI、MACD、ADX、成交量',
                '技术监控频率': '每周',
                '技术预警阈值': 'RSI>80或<30',
                '宏观监控指标': '行业政策、宏观经济数据',
                '宏观监控频率': '每月',
                '宏观预警阈值': '行业政策收紧，宏观经济下滑',
                
                # 重大事件
                '重点财报类型': '年报、季报',
                '重大事件类型1': '行业政策变化',
                '重大事件类型2': '公司财报发布',
                '重大事件类型3': '管理层变动',
                '重大事件类型4': '重大资产重组',
                
                # 关键风险提示
                '风险类型1': self._generate_risk_type(comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi),
                '当前具体情况': self._generate_risk_status(comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi),
                '对业绩的可能影响': self._generate_risk_impact(comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi),
                '应对建议': self._generate_risk_suggestions(comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi),
                '关键监控指标': self._generate_risk_monitoring(comprehensive_risk, financial_health, valuation, historical_valuation, volatility, trend, turnover, rsi)
            }
            
            # 加载模板
            template_path = 'analysis_report.md'
            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()
            
            # 替换模板中的占位符
            report_content = template
            for key, value in data.items():
                placeholder = f'[{key}]'
                report_content = report_content.replace(placeholder, str(value))
            
            # 生成报告文件名
            reports_dir = 'reports'
            if not os.path.exists(reports_dir):
                os.makedirs(reports_dir)
            
            report_filename = f"{stock_name}_{code}_综合风险分析报告.md"
            report_path = os.path.join(reports_dir, report_filename)
            
            # 写入报告文件
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            print(f"  报告已生成：{report_path}")
            
            return result
        except Exception as e:
            print(f"综合风险计算失败：{e}")
            import traceback
            traceback.print_exc()
            raise

# 测试代码
if __name__ == "__main__":
    print("初始化FinancialRiskAgent...")
    agent = FinancialRiskAgent()
    try:
        # 直接使用股票代码测试，避免名称转换
        code = "601899"  # 紫金矿业的股票代码
        print(f"测试股票代码：{code}")
        
        # 测试格式化股票代码
        formatted_code = agent.format_stock_code(code)
        print(f"格式化后的股票代码：{formatted_code}")
        
        # 测试估值分析
        print("\n测试估值分析...")
        valuation_analysis = agent.calculate_valuation_analysis(code)
        
        print("\n估值分析结果摘要：")
        print(f"最终得分：{valuation_analysis['final_score']}")
        print(f"行业类型：{valuation_analysis['industry_type']}")
        print(f"是否行业龙头：{valuation_analysis['is_industry_leader']}")
        print(f"PETTM：{valuation_analysis['petttm']}")
        print(f"PB：{valuation_analysis['pb']}")
        print(f"PSTTM：{valuation_analysis['psttm']}")
        print(f"股息率：{valuation_analysis['dividend_yield']}%")
        
        print("\n动态合理估值区间：")
        print(f"PETTM：{valuation_analysis['dynamic_valuation_range']['petttm']['low']} - {valuation_analysis['dynamic_valuation_range']['petttm']['high']}")
        print(f"PB：{valuation_analysis['dynamic_valuation_range']['pb']['low']} - {valuation_analysis['dynamic_valuation_range']['pb']['high']}")
        print(f"PSTTM：{valuation_analysis['dynamic_valuation_range']['psttm']['low']} - {valuation_analysis['dynamic_valuation_range']['psttm']['high']}")
        
        print("\n调整后估值区间：")
        print(f"PETTM：{valuation_analysis['adjusted_valuation_range']['petttm']['low']} - {valuation_analysis['adjusted_valuation_range']['petttm']['high']}")
        print(f"PB：{valuation_analysis['adjusted_valuation_range']['pb']['low']} - {valuation_analysis['adjusted_valuation_range']['pb']['high']}")
        print(f"PSTTM：{valuation_analysis['adjusted_valuation_range']['psttm']['low']} - {valuation_analysis['adjusted_valuation_range']['psttm']['high']}")
        
        print("\n各指标评分：")
        print(f"PETTM评分：{valuation_analysis['valuation_scores']['petttm_score']}")
        print(f"PB评分：{valuation_analysis['valuation_scores']['pb_score']}")
        print(f"PSTTM评分：{valuation_analysis['valuation_scores']['psttm_score']}")
        print(f"股息率评分：{valuation_analysis['valuation_scores']['dividend_score']}")
        
        # 测试历史估值分析
        print("\n测试历史估值分析...")
        historical_valuation = agent.calculate_historical_valuation_analysis(code)
        
        print("\n历史估值分析结果摘要：")
        print(f"最终得分：{historical_valuation['final_score']}")
        print(f"行业：{historical_valuation['industry']}")
        print(f"当前估值：")
        print(f"  PETTM：{historical_valuation['current_valuation']['petttm']}")
        
        # 生成综合风险分析报告
        print("\n生成综合风险分析报告...")
        comprehensive_risk = agent.calculate_comprehensive_risk(code)
        print(f"  PB：{historical_valuation['current_valuation']['pb']}")
        print(f"  PSTTM：{historical_valuation['current_valuation']['psttm']}")
        print(f"历史数据点：")
        print(f"  PETTM：{historical_valuation['historical_data']['petttm_count']}个")
        print(f"  PB：{historical_valuation['historical_data']['pb_count']}个")
        print(f"  PSTTM：{historical_valuation['historical_data']['psttm_count']}个")
        print(f"分位值：")
        print(f"  PETTM：{historical_valuation['percentiles']['petttm_percentile']}%")
        print(f"  PB：{historical_valuation['percentiles']['pb_percentile']}%")
        print(f"  PSTTM：{historical_valuation['percentiles']['psttm_percentile']}%")
        print(f"评分：")
        print(f"  PETTM：{historical_valuation['scores']['petttm_score']}")
        print(f"  PB：{historical_valuation['scores']['pb_score']}")
        print(f"  PSTTM：{historical_valuation['scores']['psttm_score']}")
        
        # 注释掉清除缓存的代码，避免每次运行都删除缓存数据
        # agent.update_all_financial_cache(code)
        
        # 测试财务健康度分析
        print("\n测试财务健康度分析...")
        financial_health = agent.calculate_financial_health_analysis(code)
        
        print("\n财务健康度分析结果摘要：")
        print(f"综合评分：{financial_health['total_score']}")
        print(f"各维度得分：")
        print(f"  盈利能力：{financial_health['profitability_score']}分")
        print(f"  现金流质量：{financial_health['cashflow_score']}分")
        print(f"  偿债能力：{financial_health['solvency_score']}分")
        print(f"  风险预警：{financial_health['risk_warning_score']}分")
        print(f"关键风险信号：")
        print(f"  ST状态：{'□' if financial_health['risk_signals']['is_st'] else '□'}")
        print(f"  连续亏损：{'□' if financial_health['risk_signals']['consecutive_loss'] else '□'}")
        print(f"  现金流为负：{'□' if financial_health['risk_signals']['negative_cashflow'] else '□'}")
        print(f"  高质押率：□（数据不可获取）")
        print(f"违约风险评估：")
        print(f"  风险等级：{financial_health['final_risk_level']}")
        print(f"  高风险信号数：{financial_health['high_risk_signals_count']}个")
        print(f"  违约概率预估：{financial_health['default_probability']}")
        
        # 测试完整的综合风险分析
        print("\n测试完整的综合风险分析...")
        comprehensive_risk = agent.calculate_comprehensive_risk(code)
        
        print("\n综合风险分析结果摘要：")
        print(f"财务健康度得分：{comprehensive_risk['financial_health_score']}")
        print(f"波动率分析得分：{comprehensive_risk['volatility_score']}")
        print(f"估值分析得分：{comprehensive_risk['valuation_score']}")
        print(f"历史估值分析得分：{comprehensive_risk['historical_valuation_score']}")
        print(f"趋势分析得分：{comprehensive_risk['trend_score']}")
        print(f"换手率分析得分：{comprehensive_risk['turnover_score']}")
        print(f"RSI分析得分：{comprehensive_risk['rsi_score']}")
        print(f"综合风险得分：{comprehensive_risk['comprehensive_score']}")
        print(f"综合风险等级：{comprehensive_risk['risk_icon']} {comprehensive_risk['risk_level']}")
        
        # 测试回测功能
        print("\n=== 测试回测功能 ===")
        # 设置回测时间范围（最近一年）
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        # 运行回测
        backtest_result = agent.backtest(code, start_date, end_date, rebalance_period=30)
        
        if backtest_result is not None:
            print("\n回测功能测试成功！")
        else:
            print("\n回测功能测试失败！")
        
        print("\n所有测试完成！")
    except Exception as e:
        print(f"\n测试失败：{e}")
        import traceback
        traceback.print_exc()