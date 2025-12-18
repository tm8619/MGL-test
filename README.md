# MGL-test
Multiple Granularity Lockingの検証をする

## 概要

このプロジェクトは、MySQLにおけるMultiple Granularity Locking (MGL) を検証するためのシンプルなGoアプリケーションです。

MGLは階層的なロックシステムで、以下のレベルでロックを管理します:
- **データベースレベル**
- **テーブルレベル** (意図ロック: IS, IX)
- **行レベル** (共有ロック: S, 排他ロック: X)

## 必要要件

- Docker & Docker Compose
- Go 1.21以上

## セットアップと実行

### 1. MySQLコンテナの起動

```bash
docker-compose up -d
```

MySQLが起動するまで少し待ちます（healthcheckで確認可能）。

### 2. Go依存関係のインストール

```bash
go mod download
```

### 3. アプリケーションの実行

```bash
go run main.go
```

## 検証シナリオ

アプリケーションは以下のMGLシナリオを検証します:

### シナリオ1: 行レベルロック
- `SELECT ... FOR UPDATE`で排他ロック (X) を取得
- 自動的にテーブルレベルで意図排他ロック (IX) が設定される

### シナリオ2: 共有ロック
- `SELECT ... LOCK IN SHARE MODE`で共有ロック (S) を取得
- 自動的にテーブルレベルで意図共有ロック (IS) が設定される

### シナリオ3: ロック情報の表示
- `performance_schema.data_locks`からロック情報を表示
- ロック階層の可視化

## クリーンアップ

```bash
docker-compose down -v
```

## ロックモードの互換性

| 要求 | IS | IX | S  | X  |
|------|----|----|----|----|
| IS   | ✓  | ✓  | ✓  | ✗  |
| IX   | ✓  | ✓  | ✗  | ✗  |
| S    | ✓  | ✗  | ✓  | ✗  |
| X    | ✗  | ✗  | ✗  | ✗  |

- **IS (Intent Shared)**: 意図共有ロック
- **IX (Intent Exclusive)**: 意図排他ロック
- **S (Shared)**: 共有ロック
- **X (Exclusive)**: 排他ロック
