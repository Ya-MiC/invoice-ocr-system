#!/usr/bin/env python3
"""依赖协议扫描工具 - 检查所有依赖是否符合宽松协议要求"""

import subprocess
import sys

ALLOWED_LICENSES = {
    'MIT', 'Apache-2.0', 'Apache 2.0', 'Apache Software License',
    'BSD-3-Clause', 'BSD-2-Clause', 'BSD', 'ISC',
    'Unlicense', 'PIL-Spirits', 'Python-2.0'
}

FORBIDDEN_LICENSES = {
    'GPL', 'GPL-2.0', 'GPL-3.0', 'LGPL', 'LGPL-2.1', 'LGPL-3.0',
    'AGPL', 'AGPL-3.0', 'AGPL-3.0-or-later'
}

def get_installed_packages():
    """获取已安装包列表"""
    result = subprocess.run(
        ['pip', 'list', '--format=json'],
        capture_output=True, text=True
    )
    import json
    return json.loads(result.stdout) if result.stdout else []

def get_package_license(package_name):
    """获取单个包的许可证"""
    result = subprocess.run(
        ['pip', 'show', package_name],
        capture_output=True, text=True
    )
    for line in result.stdout.split('\n'):
        if line.startswith('License:'):
            return line.split(':', 1)[1].strip()
    return 'Unknown'

def check_licenses():
    """检查所有依赖的许可证"""
    packages = get_installed_packages()
    violations = []
    warnings = []
    
    print("=" * 60)
    print("依赖协议扫描报告")
    print("=" * 60)
    print(f"扫描包数量: {len(packages)}\n")
    
    for pkg in packages:
        name = pkg['name']
        license_str = get_package_license(name)
        
        # 检查是否为禁止的协议
        is_forbidden = any(f in license_str for f in FORBIDDEN_LICENSES)
        is_allowed = any(a in license_str for a in ALLOWED_LICENSES)
        
        status = "✓"
        if is_forbidden:
            status = "✗"
            violations.append((name, license_str))
        elif not is_allowed:
            status = "?"
            warnings.append((name, license_str))
        
        print(f"{status} {name:30} {license_str}")
    
    print("\n" + "=" * 60)
    
    if violations:
        print(f"❌ 发现 {len(violations)} 个违规依赖:")
        for name, lic in violations:
            print(f"   - {name}: {lic}")
        return 1
    
    if warnings:
        print(f"⚠️ 发现 {len(warnings)} 个需要人工确认的依赖:")
        for name, lic in warnings:
            print(f"   - {name}: {lic}")
    
    print("✓ 所有依赖符合宽松协议要求")
    return 0

if __name__ == '__main__':
    sys.exit(check_licenses())
