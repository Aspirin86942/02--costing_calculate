r"""Windows long-path detection helper for long-path regression tests.

长路径回归测试的前提是「Win32 MAX_PATH=260 限制仍生效,无 `\\?\` 前缀的逻辑路径无法访问 >260 字符的文件」。
当系统启用 Win32 长路径(HKLM\SYSTEM\CurrentControlSet\Control\FileSystem\LongPathsEnabled=1)后,
该限制解除,无前缀逻辑路径也能访问长路径文件,回归前提不再成立。
此时相关测试应被跳过而非报错失败——这是语义正确的跳过,而非掩盖缺陷。
"""

from __future__ import annotations

import os


def win_long_paths_enabled() -> bool:
    """True 表示系统已启用 Win32 长路径(MAX_PATH=260 限制已解除)。

    非 Windows 平台返回 False(长路径回归测试本身在非 Windows 上已由 `os.name != 'nt'` 跳过)。
    读取注册表失败时返回 False,保守地保留回归覆盖。
    """
    if os.name != 'nt':
        return False
    try:
        import winreg

        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r'SYSTEM\CurrentControlSet\Control\FileSystem',
        )
        try:
            value, _ = winreg.QueryValueEx(key, 'LongPathsEnabled')
        finally:
            key.Close()
    except OSError:
        return False
    return value == 1
