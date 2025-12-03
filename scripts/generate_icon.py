"""
生成数仓主题图标
"""
from PIL import Image, ImageDraw, ImageFont
import os

def create_icon():
    """创建数仓主题图标"""
    # 创建256x256的图像
    size = 256
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    # 背景渐变（深蓝到紫色）
    for i in range(size):
        ratio = i / size
        r = int(102 * (1 - ratio) + 118 * ratio)
        g = int(126 * (1 - ratio) + 75 * ratio)
        b = int(234 * (1 - ratio) + 162 * ratio)
        draw.rectangle([(0, i), (size, i+1)], fill=(r, g, b, 255))
    
    # 绘制圆角矩形背景
    margin = 20
    corner_radius = 30
    draw.rounded_rectangle(
        [(margin, margin), (size-margin, size-margin)],
        radius=corner_radius,
        fill=(255, 255, 255, 30)
    )
    
    # 绘制数据库图标（三层）
    center_x = size // 2
    layer_height = 20
    layer_width = 120
    layer_gap = 15
    start_y = 60
    
    # 三层数据库
    colors = [
        (102, 126, 234, 255),  # ODS - 蓝色
        (118, 75, 162, 255),   # DWD - 紫色
        (237, 137, 54, 255)    # DWS - 橙色
    ]
    
    for i, color in enumerate(colors):
        y = start_y + i * (layer_height + layer_gap)
        
        # 绘制椭圆顶部
        draw.ellipse(
            [(center_x - layer_width//2, y - 10),
             (center_x + layer_width//2, y + 10)],
            fill=color
        )
        
        # 绘制矩形主体
        draw.rectangle(
            [(center_x - layer_width//2, y),
             (center_x + layer_width//2, y + layer_height)],
            fill=color
        )
        
        # 绘制椭圆底部
        draw.ellipse(
            [(center_x - layer_width//2, y + layer_height - 10),
             (center_x + layer_width//2, y + layer_height + 10)],
            fill=color
        )
        
        # 添加高光
        highlight_color = tuple([min(255, c + 40) for c in color[:3]] + [200])
        draw.ellipse(
            [(center_x - layer_width//2 + 5, y - 8),
             (center_x + layer_width//2 - 5, y + 8)],
            fill=highlight_color
        )
    
    # 绘制连接箭头
    arrow_x = center_x + layer_width//2 + 20
    for i in range(2):
        y1 = start_y + i * (layer_height + layer_gap) + layer_height//2
        y2 = start_y + (i+1) * (layer_height + layer_gap) + layer_height//2
        
        # 箭头线
        draw.line([(arrow_x, y1), (arrow_x, y2)], fill=(255, 255, 255, 200), width=3)
        
        # 箭头头部
        draw.polygon([
            (arrow_x, y2),
            (arrow_x - 6, y2 - 8),
            (arrow_x + 6, y2 - 8)
        ], fill=(255, 255, 255, 200))
    
    # 绘制文字 "DW"
    try:
        # 尝试使用系统字体
        font = ImageFont.truetype("arial.ttf", 60)
    except:
        font = ImageFont.load_default()
    
    text = "DW"
    # 获取文字边界框
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    text_x = (size - text_width) // 2
    text_y = size - 80
    
    # 绘制文字阴影
    draw.text((text_x + 2, text_y + 2), text, font=font, fill=(0, 0, 0, 100))
    # 绘制文字
    draw.text((text_x, text_y), text, font=font, fill=(255, 255, 255, 255))
    
    # 保存为多种尺寸
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    build_dir = os.path.join(base_dir, 'build')
    os.makedirs(build_dir, exist_ok=True)
    
    # 保存PNG
    png_path = os.path.join(build_dir, 'icon.png')
    img.save(png_path, 'PNG')
    print(f"✓ 已生成PNG图标: {png_path}")
    
    # 生成ICO（多尺寸）
    ico_path = os.path.join(build_dir, 'icon.ico')
    sizes = [(256, 256), (128, 128), (64, 64), (48, 48), (32, 32), (16, 16)]
    icons = []
    for size_tuple in sizes:
        resized = img.resize(size_tuple, Image.Resampling.LANCZOS)
        icons.append(resized)
    
    icons[0].save(ico_path, format='ICO', sizes=[s for s in sizes])
    print(f"✓ 已生成ICO图标: {ico_path}")
    print(f"\n图标已生成！包含尺寸: {', '.join([f'{s[0]}x{s[1]}' for s in sizes])}")


if __name__ == '__main__':
    try:
        create_icon()
        print("\n✓ 图标生成成功！")
        print("现在可以运行 'npm run build' 重新打包应用")
    except Exception as e:
        print(f"✗ 图标生成失败: {e}")
        print("\n请确保已安装 Pillow 库:")
        print("pip install Pillow")
