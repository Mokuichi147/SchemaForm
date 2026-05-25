# SchemaForm

非プログラマでもUIでフォームを作成し、公開URLで入力を受け付ける小さなフォームサービスです。

## 主な機能
- フォーム作成/編集（ブロック追加 + ドラッグ並び替え）
- 共有URLによる入力ページ
- 送信一覧（検索/フィルタ/ページネーション）
- エクスポート（Excel/CSV/Parquet/JSON、フィルタ結果のみ）
- ファイルアップロード（ローカル保存）
- 保存先の切替（SQLite/JSONファイル）
- REST API（フォーム作成/更新・送信・一覧取得）

## 起動方法（uv）
```bash
uv sync --locked

# 起動
uv run schemaform

# host/port を指定する場合
uv run schemaform --host 127.0.0.1 --port 9000
```

依存関係を更新したい場合は `uv lock` を実行してください。

ブラウザで `http://localhost:8000/admin/forms` を開いてください。

## REST API（抜粋）
```bash
# フォーム一覧
curl http://localhost:8000/api/forms

# フォーム作成（schema_json直接）
curl -X POST http://localhost:8000/api/forms \\
  -H 'Content-Type: application/json' \\
  -d '{"name":"アンケート","schema_json":{"type":"object","properties":{"name":{"type":"string"}}}}'

# フォーム送信（public_id）
curl -X POST http://localhost:8000/api/public/forms/<public_id>/submissions \\
  -H 'Content-Type: application/json' \\
  -d '{"data_json":{"name":"太郎"}}'

# 送信一覧（cursor）
curl -i "http://localhost:8000/api/forms/<form_id>/submissions?limit=50"
```

## 環境変数
他サービスと衝突しないよう、すべて `SCHEMAFORM_` プレフィックスを付けています。

- `SCHEMAFORM_STORAGE_BACKEND=sqlite|json`
- `SCHEMAFORM_SQLITE_PATH=./data/app.db`
- `SCHEMAFORM_JSON_PATH=./data/jsonstore.json`
- `SCHEMAFORM_UPLOAD_DIR=./data/uploads`
- `SCHEMAFORM_UPLOAD_MAX_BYTES`（未指定なら無制限）
- `SCHEMAFORM_FILE_URL_SECRET=./data/file_url.secret`（`/files/{file_id}` の署名付きURL生成に使う秘密鍵ファイル。未存在なら自動生成）
- `SCHEMAFORM_FILE_URL_TTL_SECONDS=86400`（署名付きファイルURLの有効期間。デフォルト24時間）
- `SCHEMAFORM_PASSWORD_MIN_LENGTH=8`（アカウント作成・パスワード変更時の最小文字数）
- `SCHEMAFORM_HOST=0.0.0.0`
- `SCHEMAFORM_PORT=8000`

## JsonSchema 対応範囲
- `string | number | integer | boolean | enum | array(items=primitive|file)`
- ファイルは `format=binary` の `string` として扱い、内部的には `file_id` を保持します。
- 配列はプリミティブ/ファイルのみ（入れ子のobjectは非対応）

## ライセンス
- MIT OR Apache-2.0
