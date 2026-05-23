# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all, collect_data_files

block_cipher = None

datas = []
binaries = []
hiddenimports = []

# templates / static をバンドルに含める
datas += [
    ("templates", "templates"),
    ("static", "static"),
]

# 動的インポートを多用するパッケージを丸ごと収集
for _pkg in [
    "uvicorn",
    "fastapi",
    "starlette",
    "aiosqlite",
    "sqlalchemy",
    "jsonschema",
    "user_permission",
    "openpyxl",
    "pyarrow",
]:
    _d, _b, _h = collect_all(_pkg)
    datas += _d
    binaries += _b
    hiddenimports += _h

a = Analysis(
    ["src/schemaform/__main__.py"],
    pathex=["src"],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports
    + [
        "sqlalchemy.dialects.sqlite",
        "sqlalchemy.dialects.sqlite.aiosqlite",
        "multipart",
        "jinja2",
        "jinja2.ext",
        "orjson",
        "ulid",
        "typer",
        "filelock",
        "tinydb",
        "tinydb.storages",
        "tinydb.middlewares",
        "httpx",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="schemaform",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
