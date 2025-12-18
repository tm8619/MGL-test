# MGL-test

Multiple Granularity Locking の検証をする

## 概要

このプロジェクトは、MySQL における Multiple Granularity Locking (MGL) を検証するためのシンプルな Go アプリケーションです。

MGL は階層的なロックシステムで、以下のレベルでロックを管理します:

- **データベースレベル**
- **テーブルレベル** (意図ロック: IS, IX)
- **行レベル** (共有ロック: S, 排他ロック: X)

## 必要要件

- Docker & Docker Compose
- Go 1.25 以上

## セットアップと実行

### 1. MySQL コンテナの起動

```bash
docker-compose up -d
```

Docker デーモン（例: Docker Desktop）が起動していないとコンテナは立ち上がらないので注意してください。

MySQL が起動するまで少し待ちます（healthcheck で確認可能）。

### 2. Go 依存関係のインストール

```bash
go mod download
```

### 3. アプリケーションの実行

```bash
go run main.go
```

## 階層ロック(User/Account/Resource)の単体テスト

このリポジトリには、MySQL の行ロック（`FOR SHARE` / `FOR UPDATE NOWAIT`）を使った
`User -> Account -> Resource` の階層ロック検証用テストが入っています（パッケージ: `hierlock`）。

### 事前準備

- MySQL 8.0 が起動していること（`docker-compose up -d` 推奨）
- 接続先を変えたい場合は `MYSQL_DSN` を指定

例:

```bash
export MYSQL_DSN='testuser:testpassword@tcp(127.0.0.1:3306)/mgl_test?parseTime=true&multiStatements=true'
```

### 実行

```bash
go test -v ./...
```

MySQL に接続できない環境では、`hierlock` のテストは `SKIP` になります（他のテストは動きます）。

## 検証シナリオ

アプリケーションは以下の MGL シナリオを検証します:

### シナリオ 1: 行レベルロック

- `SELECT ... FOR UPDATE`で排他ロック (X) を取得
- 自動的にテーブルレベルで意図排他ロック (IX) が設定される

### シナリオ 2: 共有ロック

- `SELECT ... LOCK IN SHARE MODE`で共有ロック (S) を取得
- 自動的にテーブルレベルで意図共有ロック (IS) が設定される

### シナリオ 3: ロック情報の表示

- `performance_schema.data_locks`からロック情報を表示
- ロック階層の可視化

## クリーンアップ

```bash
docker-compose down -v
```

## ロックモードの互換性

| 要求 | IS  | IX  | S   | X   |
| ---- | --- | --- | --- | --- |
| IS   | ✓   | ✓   | ✓   | ✗   |
| IX   | ✓   | ✓   | ✗   | ✗   |
| S    | ✓   | ✗   | ✓   | ✗   |
| X    | ✗   | ✗   | ✗   | ✗   |

- **IS (Intent Shared)**: 意図共有ロック
- **IX (Intent Exclusive)**: 意図排他ロック
- **S (Shared)**: 共有ロック
- **X (Exclusive)**: 排他ロック
