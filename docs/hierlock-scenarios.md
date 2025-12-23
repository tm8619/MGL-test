# User / Account / Resource 階層ロック: シナリオと期待挙動

このドキュメントは、このリポジトリの `hierlock` パッケージが実装している
「User → Account → Resource」の階層ロックについて、具体的なシナリオごとの期待挙動をまとめたものです。

## 前提: ルール

### 階層

- User
- Account（User 配下）
- Resource（Account 配下）

### ロック取得ルール

ロック対象の**上位階層は共有ロック**、対象自身は**排他ロック**とします。

例: `Resource(u1/a1/r1)` をロックする場合

- `User(u1)` : 共有ロック
- `Account(u1/a1)` : 共有ロック
- `Resource(u1/a1/r1)` : 排他ロック

### 実装方式（MySQL 行ロック）

`hier_locks(lock_key PRIMARY KEY)` というテーブルの行を、以下の SQL でロックします。

- 共有ロック: `SELECT ... FOR SHARE`
- 排他ロック: `SELECT ... FOR UPDATE`

NOWAIT は使わないため、競合時は**エラーではなくブロック（待ち）**が基本挙動です。

## シナリオ集（期待挙動）

### 1) 同一 Resource の二重取得

- T1: `Resource(u1/a1/r1)` を取得
- T2: `Resource(u1/a1/r1)` を取得

期待:

- T2 は `Resource(u1/a1/r1)` の排他ロックで待たされる
- T1 解放後に T2 が取得できる

対応テスト:

- `TestHierarchy_CompatibilityMatrix/resource blocks same resource`

### 2) 同一 Account 配下の別 Resource 同士

- T1: `Resource(u1/a1/r1)`
- T2: `Resource(u1/a1/r2)`

期待:

- `User(u1)` と `Account(u1/a1)` は共有ロック同士なので両立
- `Resource` は別行なので両立
- よって **待たずに両方取得できる**

対応テスト:

- `TestHierarchy_CompatibilityMatrix/resource allows different resource same account`

### 3) Account ロックと配下 Resource の競合

- T1: `Account(u1/a1)`
- T2: `Resource(u1/a1/r1)`

期待:

- `Account(u1/a1)` は排他
- `Resource(u1/a1/r1)` は `Account(u1/a1)` を共有で取りに行く
- 排他 vs 共有で競合するため **T2 が待つ**

対応テスト:

- `TestHierarchy_CompatibilityMatrix/account blocks resource under same account`

（逆順も同様に、Resource が先に `Account(u1/a1)` を共有で保持していると、Account 排他が待つ）

### 4) User ロックと配下 Account/Resource の競合

- T1: `User(u1)`
- T2: `Account(u1/a1)` or `Resource(u1/a1/r1)`

期待:

- `User(u1)` は排他
- 配下は `User(u1)` を共有で取りに行く
- 排他 vs 共有で競合するため **T2 が待つ**

対応テスト:

- `TestHierarchy_CompatibilityMatrix/user blocks account under same user`
- `TestHierarchy_CompatibilityMatrix/user blocks resource under same user`

### 5) 別 User は独立

- T1: `User(u1)`
- T2: `User(u2)`（または `Account(u2/...)` / `Resource(u2/...)`）

期待:

- ロックする行（lock_key）が別なので **両立**

対応テスト:

- `TestHierarchy_CompatibilityMatrix/user allows different user`

## デッドロック: 「悪い取り方」の対照実験

### 6) 複数 Resource を“順序を揃えず”に取るとデッドロックしうる

- T1: `Resource(u1/a1/r1)` を取ってから `Resource(u1/a1/r2)` を取る
- T2: `Resource(u1/a1/r2)` を取ってから `Resource(u1/a1/r1)` を取る

期待:

- T1 は `r1` を保持し、T2 は `r2` を保持
- その後お互いが相手の Resource を取りに行き、循環待ちになる
- MySQL が検知し、どちらかが `ER_LOCK_DEADLOCK (1213)`（または設定により `Lock wait timeout (1205)`）で失敗する

対応テスト:

- `TestHierarchy_Deadlock_UnorderedMultiResource`

### 7) 回避策: Resource は必ず同一順序で取る

`AcquireResources` は `resourceIDs` をソートして、どのトランザクションも同じ順序で Resource をロックします。
これにより循環待ちが起きにくくなります。

対応テスト:

- `TestHierarchy_NoDeadlock_MultiResourceOrdered`

## 網羅的マトリクス

`Level × IDパターン` の多数の組み合わせについて、
「先に取ったロックを保持した状態で、後から取るロックがブロックするか」を自動生成して検証します。

対応テスト:

- `TestHierarchy_ExhaustiveGeneratedMatrix`
