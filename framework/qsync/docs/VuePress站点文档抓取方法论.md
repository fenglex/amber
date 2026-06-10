# VuePress 站点全站文档抓取方法论

> 基于 https://help.tdx.com.cn/quant/ 项目的实战总结，适用于所有 VuePress 1.x/2.x 构建的文档站点

---

## 一、核心思路

VuePress 文档站点是 SPA（单页应用），内容通过 JavaScript 动态渲染。直接 `curl` 获取的 HTML 通常只有框架壳，正文内容为空。但 VuePress 的 SSR（服务端渲染）机制会在 HTML 中预埋内容，关键在于找到正确的 URL 格式。

### 关键发现

| URL 格式 | 内容情况 | 说明 |
|----------|----------|------|
| `/quant/docs/markdown/xxx` | 仅框架 + 标题 | 主路径返回 SPA 壳 |
| `/quant/docs/markdown/xxx.html` | **完整 SSR 内容** | 加 `.html` 后缀触发 SSR |
| `/quant/docs/markdown/xxx/` | 同主路径 | 尾部斜杠无区别 |

**核心技巧：给子页面 URL 加 `.html` 后缀即可获取 SSR 渲染的完整内容。**

---

## 二、完整抓取流程

### 步骤 1：获取站点 JS Bundle

```bash
# 访问首页，提取 JS bundle URL
curl -sL "https://help.tdx.com.cn/quant/" | grep -oP 'src="[^"]*app\.[^"]*\.js"'
# 输出示例: /quant/assets/js/app.d8d97887.js

# 下载完整 app.js
curl -sL "https://help.tdx.com.cn/quant/assets/js/app.d8d97887.js" -o app.js
```

### 步骤 2：从 app.js 提取侧边栏配置

VuePress 的侧边栏配置嵌在 `app.js` 中，格式如下：

```javascript
sidebar: {
  "/docs/markdown/mindoc-1cfsjkbf8f3is/": [
    { title: "TdxQuant概述", collapsable: false, children: [
      ["", "TdxQuant概述"],
      ["TdxQuantVersion", "版本更新说明"],
      ["mindoc-1d00970eq1rtc", "安装Python及开发环境"],
    ]},
    // ...
  ]
}
```

**提取脚本（Python）：**

```python
import re, json

with open('app.js', 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()

# 定位 sidebar: 配置
match = re.search(r'sidebar:\s*\{', content)
if not match:
    raise Exception("未找到 sidebar 配置")

# 提取 sidebar 的完整 JSON 对象
start = match.end() - 1  # 从 { 开始
depth = 0
i = start
while i < len(content):
    if content[i] == '{': depth += 1
    elif content[i] == '}': 
        depth -= 1
        if depth == 0: break
    i += 1

sidebar_text = content[start:i+1]

# 替换 JS 标识符为合法 JSON
# 1. 无引号的 key（如 collapsable: false → "collapsible": false）
sidebar_text = re.sub(r'(?<=[\{,])\s*([a-zA-Z_]\w*)\s*:', r' "\1":', sidebar_text)
# 2. 单引号 → 双引号
sidebar_text = sidebar_text.replace("'", '"')
# 3. 布尔值
sidebar_text = sidebar_text.replace('true', 'true').replace('false', 'false')

sidebar = json.loads(sidebar_text)
```

### 步骤 3：构建页面 URL 列表

从 sidebar 配置中提取所有页面路径和标题：

```python
pages = []  # [(title, url_path), ...]

for base_path, sections in sidebar.items():
    for section in sections:
        if isinstance(section, list):
            # 简单格式: [path, title]
            rel_path, title = section
            full_path = base_path + rel_path
            pages.append((title, full_path))
        elif isinstance(section, dict):
            # 带 children 的分组
            title = section.get('title', '')
            children = section.get('children', [])
            # 父页面
            pages.append((title, base_path))
            # 子页面
            for child in children:
                if isinstance(child, list):
                    child_path, child_title = child
                    full_path = base_path + child_path
                    pages.append((child_title, full_path))
```

### 步骤 4：批量抓取页面内容

**关键：子页面加 `.html` 后缀获取 SSR 内容**

```python
import urllib.request
from bs4 import BeautifulSoup
from markdownify import markdownify as md

BASE_URL = "https://help.tdx.com.cn"

def fetch_page_content(path):
    """抓取单个页面的正文内容"""
    # 主页面不加 .html，子页面加 .html
    if path.endswith('/'):
        url = BASE_URL + path
    else:
        url = BASE_URL + path + '.html'

    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode('utf-8', errors='replace')

    # 提取正文区域
    soup = BeautifulSoup(html, 'html.parser')
    content_div = soup.find('div', class_='content__default')
    if not content_div:
        content_div = soup.find('div', class_='theme-default-content')
    if not content_div:
        return ""

    # 转为 Markdown
    return md(str(content_div), heading_style="ATX", code_language="")
```

### 步骤 5：清理格式

VuePress 生成的 Markdown 有多种噪音需要清理：

```python
import re

def clean_markdown(text):
    """清理 VuePress 生成的 Markdown 噪音"""

    # 1. 移除行号噪音（代码块中每行开头的序号）
    #    VuePress highlight.js 生成的格式: "1│code" 或 "1  code"
    text = re.sub(r'^\s*\d+[│|]\s*', '', text, flags=re.MULTILINE)

    # 2. 移除转义下划线（代码块外）
    #    VuePress 将 _ 转义为 \_ 防止被解析为斜体
    def unescape_underscores_outside_codeblock(text):
        parts = re.split(r'(```[\s\S]*?```)', text)
        for i in range(len(parts)):
            if not parts[i].startswith('```'):
                parts[i] = parts[i].replace('\\_', '_')
        return ''.join(parts)
    text = unescape_underscores_outside_codeblock(text)

    # 3. 移除链接中的 "(opens new window)" 标记
    text = text.replace('(opens new window)', '')

    # 4. 移除锚点链接标记
    text = re.sub(r'\[🔗\]\(#[^)]*\)', '', text)

    # 5. 图片路径转绝对 URL
    text = re.sub(r'!\[([^\]]*)\]\(/quant/', rf'![\1](https://help.tdx.com.cn/quant/', text)

    # 6. 清理连续空行（最多保留2个）
    text = re.sub(r'\n{4,}', '\n\n\n', text)

    return text.strip()
```

### 步骤 6：按文件夹归档输出

```python
import os

def split_to_folders(pages_with_content, output_root):
    """
    将抓取内容按章节拆分为文件夹结构
    pages_with_content: [(chapter_num, chapter_title, [(sub_title, content), ...])]
    """
    os.makedirs(output_root, exist_ok=True)

    for ch_num, ch_title, subs in pages_with_content:
        ch_dir = os.path.join(output_root, f"{ch_num:02d}-{ch_title}")
        os.makedirs(ch_dir, exist_ok=True)

        # 写章节 README.md
        with open(os.path.join(ch_dir, 'README.md'), 'w', encoding='utf-8') as f:
            f.write(f"# {ch_title}\n\n")
            for si, (sub_title, _) in enumerate(subs, 1):
                filename = f"{si}-{sub_title}.md"
                f.write(f"- [{sub_title}](./{filename})\n")

        # 写子章节文件
        for si, (sub_title, content) in enumerate(subs, 1):
            filename = f"{si}-{sub_title}.md"
            with open(os.path.join(ch_dir, filename), 'w', encoding='utf-8') as f:
                # 去掉重复的顶级标题（VuePress 会生成 ## 和 # 两个重复标题）
                lines = content.split('\n')
                cleaned = skip_duplicate_headings(lines)
                f.write('\n'.join(cleaned))

    # 写总 README.md
    with open(os.path.join(output_root, 'README.md'), 'w', encoding='utf-8') as f:
        f.write("# 文档目录\n\n")
        for ch_num, ch_title, subs in pages_with_content:
            f.write(f"{ch_num}. [{ch_title}](./{ch_num:02d}-{ch_title}/)\n\n")
```

---

## 三、常见问题排查

### Q1：curl 获取的 HTML 正文为空？

**原因**：VuePress SPA 模式下，主内容区域由 JavaScript 动态渲染，`curl` 只能拿到空壳。

**解决方案**：
1. 优先尝试 `.html` 后缀 URL（触发 SSR）
2. 如果 `.html` 也不行，从 JS chunk 中提取 Vue 组件的渲染内容
3. 最终手段：使用 Playwright/Puppeteer 无头浏览器渲染

### Q2：如何找到 JS chunk 中的页面内容？

VuePress 1.x 将页面内容打包到独立 JS chunk 中：

```bash
# 列出所有 chunk 文件
curl -sL "https://help.tdx.com.cn/quant/" | grep -oP '/assets/js/\d+\.[a-f0-9]+\.js'

# 下载并搜索内容
curl -sL "https://help.tdx.com.cn/quant/assets/js/7.cbaaa1f5.js" | grep -o 'content__'
```

chunk 中的内容是 Vue 渲染函数（`_c('p',[_v("text")]`），不是直接 HTML，需要额外解析。**建议优先使用 SSR 方案。**

### Q3：如何处理代码块中的行号？

VuePress 使用 `highlight.js` 的 `hljs` 行号插件，生成的代码块格式为：

```
1│import tdx
2│from tdx import *
```

清理正则：

```python
# 移除行号前缀
text = re.sub(r'^\s*\d+[│|]\s*', '', text, flags=re.MULTILINE)
```

### Q4：sidebar 配置解析失败？

VuePress 的 JS 不是标准 JSON，需要预处理：

```python
# 1. 无引号 key → 加引号
text = re.sub(r'(?<=[\{,])\s*([a-zA-Z_]\w*)\s*:', r' "\1":', text)

# 2. 单引号 → 双引号
text = text.replace("'", '"')

# 3. 尾部逗号（JSON 不允许）
text = re.sub(r',\s*([}\]])', r'\1', text)

# 4. undefined → null
text = text.replace('undefined', 'null')
```

### Q5：子页面标题重复？

VuePress 在渲染时同时生成 `## N.X 标题` 和 `# 标题`，导致内容开头有两个重复标题。

清理策略：只删除开头的 `##` 和 `#` 级别标题，保留 `###` 及更深层级（它们是正文内容标题）：

```python
def skip_duplicate_headings(lines):
    cleaned = []
    past_top_headings = False
    for line in lines:
        stripped = line.strip()
        if not past_top_headings:
            m = re.match(r'^(#{1,6})\s+', stripped)
            if m:
                level = len(m.group(1))
                if level <= 2:
                    continue  # 跳过 ## 和 # 标题
                else:
                    past_top_headings = True
                    cleaned.append(line)
            elif stripped == '':
                continue  # 跳过标题间的空行
            else:
                past_top_headings = True
                cleaned.append(line)
        else:
            cleaned.append(line)
    return cleaned
```

---

## 四、通用适配检查清单

适用于任何 VuePress 文档站点的抓取：

- [ ] **1. 识别站点框架**：查看 HTML 源码，确认是否为 VuePress（查找 `vuepress`、`__vuepress__` 等标记）
- [ ] **2. 定位 app.js**：从首页 HTML 提取主 JS bundle URL
- [ ] **3. 提取 sidebar 配置**：从 app.js 中解析侧边栏结构（目录树）
- [ ] **4. 测试 SSR 可用性**：尝试 `.html` 后缀 URL，确认是否能获取完整内容
- [ ] **5. 批量抓取**：遍历所有页面路径，用 SSR URL 获取内容
- [ ] **6. HTML → Markdown**：使用 `markdownify` 或 `html2text` 转换
- [ ] **7. 格式清理**：处理行号、转义字符、重复标题等噪音
- [ ] **8. 文件夹归档**：按原站目录结构输出文件

---

## 五、依赖环境

```
# Python 环境（建议 venv 隔离）
pip install beautifulsoup4 markdownify

# 如需浏览器渲染（SSR 不可用时的后备方案）
pip install playwright
playwright install chromium
```

---

## 六、备选方案：Playwright 无头浏览器

当 SSR 方案不可用（如纯 CSR 站点）时，使用 Playwright 渲染：

```python
from playwright.sync_api import sync_playwright

def fetch_with_playwright(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until='networkidle')
        
        # 等待内容加载
        page.wait_for_selector('.theme-default-content', timeout=10000)
        
        # 提取正文 HTML
        content = page.query_selector('.theme-default-content')
        html = content.inner_html() if content else ""
        
        browser.close()
        return html
```

---

## 七、项目产出示例

以 TdxQuant 文档站为例：

| 项目 | 数值 |
|------|------|
| 站点 URL | https://help.tdx.com.cn/quant/ |
| 框架版本 | VuePress 1.x |
| 总页面数 | 85 |
| 顶级章节 | 15 |
| 产出文件数 | 85 个 .md |
| 产出总大小 | ~470 KB |
| 抓取耗时 | ~3 分钟（SSR 方案） |

### 文件夹结构

```
TdxQuant文档/
├── README.md                           # 总目录
├── 01-TdxQuant概述/
│   ├── README.md
│   ├── 1.1-版本更新说明.md
│   ├── 1.2-安装Python及开发环境.md
│   └── ...
├── 02-通用函数/
│   ├── README.md
│   ├── 2.1-初始化initialize.md
│   └── ...
├── ...
└── 15-常见问题/
    └── README.md
```
