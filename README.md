# JsonSchemaForm

非プログラマでもUIでフォームを作成し、公開URLで入力を受け付ける小さなフォームサービスです。

## 主な機能
- フォーム作成/編集（ブロック追加 + ドラッグ並び替え）
- 共有URLによる入力ページ
- 送信一覧（検索/フィルタ/ページネーション）
- CSV/TSVエクスポート（フィルタ結果のみ）
- ファイルアップロード（ローカル保存）
- 保存先の切替（SQLite/JSONファイル）

## 起動方法（uv）
```bash
uv sync --locked

# 起動
uv run main.py

# host/port を指定する場合
uv run main.py --host 127.0.0.1 --port 9000
```

依存関係を更新したい場合は `uv lock` を実行してください。

ブラウザで `http://localhost:8000/admin/forms` を開いてください。

## 環境変数
- `STORAGE_BACKEND=sqlite|json`
- `SQLITE_PATH=./data/app.db`
- `JSON_PATH=./data/jsonstore.json`
- `UPLOAD_DIR=./data/uploads`
- `UPLOAD_MAX_BYTES`（未指定なら無制限）
- `AUTH_MODE=none|ldap`（ldapは未実装）
- `HOST=0.0.0.0`
- `PORT=8000`

## JsonSchema 対応範囲
- `string | number | integer | boolean | enum | array(items=primitive|file)`
- ファイルは `format=binary` の `string` として扱い、内部的には `file_id` を保持します。
- 配列はプリミティブ/ファイルのみ（入れ子のobjectは非対応）

## ライセンス
- MIT OR Apache-2.0
