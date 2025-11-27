"""
修改exe文件图标
需要安装: pip install pefile
"""
import os
import sys

try:
    import pefile
    from PIL import Image
except ImportError:
    print("请先安装依赖:")
    print("pip install pefile pillow")
    sys.exit(1)


def change_exe_icon(exe_path, ico_path):
    """修改exe文件的图标"""
    if not os.path.exists(exe_path):
        print(f"错误: 找不到exe文件: {exe_path}")
        return False
    
    if not os.path.exists(ico_path):
        print(f"错误: 找不到图标文件: {ico_path}")
        return False
    
    print(f"正在修改图标...")
    print(f"EXE: {exe_path}")
    print(f"ICO: {ico_path}")
    
    try:
        # 使用ResourceHacker或rcedit工具
        # 由于Python直接修改exe图标比较复杂，建议使用外部工具
        print("\n由于技术限制，建议使用以下方法修改图标：")
        print("\n方法1: 使用Resource Hacker")
        print("1. 下载 Resource Hacker: http://www.angusj.com/resourcehacker/")
        print("2. 打开exe文件")
        print("3. 替换Icon资源")
        print("4. 保存")
        
        print("\n方法2: 使用rcedit")
        print("下载: https://github.com/electron/rcedit/releases")
        print(f"运行: rcedit.exe \"{exe_path}\" --set-icon \"{ico_path}\"")
        
        return False
        
    except Exception as e:
        print(f"错误: {e}")
        return False


if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 便携版路径
    portable_dir = os.path.join(base_dir, "电商数仓配置器-便携版-v1.0.0")
    exe_path = os.path.join(portable_dir, "电商数仓配置器.exe")
    ico_path = os.path.join(base_dir, "build", "icon.ico")
    
    change_exe_icon(exe_path, ico_path)
