"""路径配置管理"""

from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent

# 数据目录
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'

# 原始数据分类
GB_RAW_DIR = RAW_DIR / 'gb'  # GB 开头文件
SHUKONG_RAW_DIR = RAW_DIR / 'shukong'  # 数控开头文件

# 处理结果分类
GB_PROCESSED_DIR = PROCESSED_DIR / 'gb'
SHUKONG_PROCESSED_DIR = PROCESSED_DIR / 'shukong'

# 文档目录
DOCS_DIR = PROJECT_ROOT / 'docs'
FIELD_DEFS_DIR = DOCS_DIR / 'field_definitions'


def ensure_directories() -> list[Path]:
    """确保所有目录存在"""
    dirs = [GB_RAW_DIR, SHUKONG_RAW_DIR, GB_PROCESSED_DIR, SHUKONG_PROCESSED_DIR, FIELD_DEFS_DIR]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    return dirs
