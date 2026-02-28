from __future__ import annotations

import re
import os
import html as ihtml
import pandas as pd
from bs4 import BeautifulSoup


_CJK_RE = re.compile(r"[\u4e00-\u9fff]")  # 常用汉字区


def extract_chinese_titles(html_str: str) -> tuple[list[str], pd.DataFrame]:
    """
    提取所有 title="..."，并过滤掉不含中文字符的（如 FYearPeriod / FOrderBillNo）。
    返回：titles(去重保序), error_log
    """
    error_log = pd.DataFrame(columns=["stage", "message"])

    if not html_str or not html_str.strip():
        error_log.loc[len(error_log)] = ["input_check", "输入为空"]
        return [], error_log

    # 兼容 title=&quot;...&quot; 这类转义
    html_str = ihtml.unescape(html_str)

    soup = BeautifulSoup(html_str, "html.parser")
    tags = soup.find_all(attrs={"title": True})
    raw = [t.get("title") for t in tags if t.get("title") is not None]

    # 过滤：至少包含一个中文字符
    kept = [s for s in raw if _CJK_RE.search(s or "")]

    # 去重保序
    kept_unique = list(dict.fromkeys(kept))

    # Data Integrity Check
    if len(raw) == 0:
        error_log.loc[len(error_log)] = ["parse_check", "未找到任何 title 属性"]
    if len(kept_unique) == 0 and len(raw) > 0:
        error_log.loc[len(error_log)] = ["filter_check", "找到了 title，但过滤后全被剔除（可能都是英文/代码字段）"]

    return kept_unique, error_log

if __name__ == "__main__":
    # 示例用法
    html_input_path = r"D:\03- Program\02- special\02- costing_calculate\gb金蝶字段.html"
    if os.path.exists(html_input_path):
        with open(html_input_path, "r", encoding="utf-8") as f:
            html_input = f.read()
    titles, errors = extract_chinese_titles(html_input)
    print("提取的标题:", titles)
    print("错误日志:\n", errors)