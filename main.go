package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	dbUser     = "testuser"
	dbPassword = "testpassword"
	dbHost     = "localhost"
	dbPort     = "3306"
	dbName     = "mgl_test"
)

// Multiple Granularity Locking (MGL) のテスト
// MySQLのロック階層を検証:
// 1. データベースレベル
// 2. テーブルレベル (意図ロック: IS, IX)
// 3. 行レベル (共有ロック: S, 排他ロック: X)

func main() {
	fmt.Println("=== Multiple Granularity Locking 検証開始 ===")
	fmt.Println()

	// データベース接続
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		dbUser, dbPassword, dbHost, dbPort, dbName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("データベース接続エラー: %v", err)
	}
	defer db.Close()

	// 接続確認
	if err := db.Ping(); err != nil {
		log.Fatalf("データベースへのping失敗: %v", err)
	}
	fmt.Println("✓ データベース接続成功")

	// テーブルの準備
	if err := setupTable(db); err != nil {
		log.Fatalf("テーブルセットアップエラー: %v", err)
	}
	fmt.Println("✓ テーブルセットアップ完了")
	fmt.Println()

	// MGLシナリオのテスト
	fmt.Println("--- MGL検証シナリオ ---")
	fmt.Println()

	// シナリオ1: 行レベルロックの確認
	if err := testRowLevelLocking(db); err != nil {
		log.Printf("警告: 行レベルロックテスト: %v", err)
	}

	// シナリオ2: テーブルロックと行ロックの互換性
	if err := testTableAndRowLocks(db); err != nil {
		log.Printf("警告: テーブルと行ロックテスト: %v", err)
	}

	// シナリオ3: 並行トランザクションのロック競合
	if err := demonstrateConcurrentLocking(db); err != nil {
		log.Printf("警告: 並行ロックテスト: %v", err)
	}

	// シナリオ4: ロック情報の表示
	if err := displayLockInfo(db); err != nil {
		log.Printf("警告: ロック情報表示: %v", err)
	}

	fmt.Println()
	fmt.Println("=== MGL検証完了 ===")
}

// テーブルのセットアップ
func setupTable(db *sql.DB) error {
	// 既存のテーブルを削除
	_, err := db.Exec("DROP TABLE IF EXISTS test_table")
	if err != nil {
		return fmt.Errorf("テーブル削除エラー: %w", err)
	}

	// 新しいテーブルを作成
	createTableSQL := `
		CREATE TABLE test_table (
			id INT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(100),
			value INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB
	`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("テーブル作成エラー: %w", err)
	}

	// テストデータの挿入
	insertSQL := "INSERT INTO test_table (name, value) VALUES (?, ?)"
	for i := 1; i <= 5; i++ {
		_, err = db.Exec(insertSQL, fmt.Sprintf("Item-%d", i), i*10)
		if err != nil {
			return fmt.Errorf("データ挿入エラー: %w", err)
		}
	}

	return nil
}

// シナリオ1: 行レベルロックの確認
func testRowLevelLocking(db *sql.DB) error {
	fmt.Println("シナリオ1: 行レベルロック (FOR UPDATE)")

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("トランザクション開始エラー: %w", err)
	}
	defer tx.Rollback()

	// SELECT ... FOR UPDATE で排他ロック (X) を取得
	var id int
	var name string
	var value int
	err = tx.QueryRow("SELECT id, name, value FROM test_table WHERE id = 1 FOR UPDATE").Scan(&id, &name, &value)
	if err != nil {
		return fmt.Errorf("行ロック取得エラー: %w", err)
	}

	fmt.Printf("  ロック取得: id=%d, name=%s, value=%d\n", id, name, value)
	fmt.Println("  → 行レベルで排他ロック (X) を取得")
	fmt.Println("  → テーブルレベルでは意図排他ロック (IX) が自動設定")

	// 他のトランザクションはこの行を読み取ることができない (FOR UPDATEの場合)
	// しかし、他の行は自由にアクセス可能

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("コミットエラー: %w", err)
	}

	fmt.Println("  ✓ トランザクション完了、ロック解放")
	fmt.Println()
	return nil
}

// シナリオ2: テーブルロックと行ロックの互換性
func testTableAndRowLocks(db *sql.DB) error {
	fmt.Println("シナリオ2: 複数行への共有ロック")

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("トランザクション開始エラー: %w", err)
	}
	defer tx.Rollback()

	// SELECT ... LOCK IN SHARE MODE で各行に共有ロック (S) を取得
	rows, err := tx.Query("SELECT id, name FROM test_table WHERE id <= 3 LOCK IN SHARE MODE")
	if err != nil {
		return fmt.Errorf("共有ロック取得エラー: %w", err)
	}
	defer rows.Close()

	count := 0
	fmt.Println("  共有ロック取得:")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return err
		}
		fmt.Printf("    id=%d, name=%s\n", id, name)
		count++
	}

	fmt.Printf("  → %d行に共有ロック (S) を取得\n", count)
	fmt.Println("  → テーブルレベルでは意図共有ロック (IS) が自動設定")
	fmt.Println("  → 他のトランザクションは読み取り可能、書き込みはブロック")

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("コミットエラー: %w", err)
	}

	fmt.Println("  ✓ トランザクション完了、ロック解放")
	fmt.Println()
	return nil
}

// シナリオ4: ロック情報の表示
func displayLockInfo(db *sql.DB) error {
	fmt.Println("シナリオ4: 現在のロック情報")

	// performance_schemaからロック情報を取得
	// MySQLのロック階層を確認
	lockQuery := `
		SELECT 
			OBJECT_SCHEMA,
			OBJECT_NAME,
			LOCK_TYPE,
			LOCK_MODE,
			LOCK_STATUS
		FROM performance_schema.data_locks
		WHERE OBJECT_SCHEMA = ?
		LIMIT 10
	`

	rows, err := db.Query(lockQuery, dbName)
	if err != nil {
		// performance_schemaが有効でない場合はスキップ
		fmt.Println("  (ロック情報の取得をスキップ - performance_schemaが無効の可能性)")
		fmt.Println()
		return nil
	}
	defer rows.Close()

	fmt.Println("  現在のロック:")
	hasLocks := false
	for rows.Next() {
		var schema, table, lockType, lockMode, lockStatus string
		if err := rows.Scan(&schema, &table, &lockType, &lockMode, &lockStatus); err != nil {
			return fmt.Errorf("ロック情報の読み取りエラー: %w", err)
		}
		fmt.Printf("    %s.%s - Type: %s, Mode: %s, Status: %s\n",
			schema, table, lockType, lockMode, lockStatus)
		hasLocks = true
	}

	if !hasLocks {
		fmt.Println("    (現在アクティブなロックはありません)")
	}

	fmt.Println()
	return nil
}

// シナリオ3: 並行トランザクションのシミュレーション
func demonstrateConcurrentLocking(db *sql.DB) error {
	fmt.Println("シナリオ3: 並行トランザクションでのロック競合")
	fmt.Println()

	// トランザクション1: 行をロック
	tx1, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx1.Rollback()

	var value int
	err = tx1.QueryRow("SELECT value FROM test_table WHERE id = 1 FOR UPDATE").Scan(&value)
	if err != nil {
		return err
	}
	fmt.Println("  TX1: id=1 の行に排他ロックを取得")

	// トランザクション2を別のゴルーチンで実行
	done := make(chan error, 1)
	go func() {
		tx2, err := db.Begin()
		if err != nil {
			done <- err
			return
		}
		defer tx2.Rollback() // エラー時も確実にクリーンアップ

		fmt.Println("  TX2: id=1 の行へのアクセスを試行...")
		// この操作はTX1がコミット/ロールバックするまでブロックされる
		var value2 int
		err = tx2.QueryRow("SELECT value FROM test_table WHERE id = 1 FOR UPDATE").Scan(&value2)
		if err != nil {
			done <- err
			return
		}
		
		// 成功時はコミット
		if err := tx2.Commit(); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	// TX1を少し待ってからコミット
	time.Sleep(2 * time.Second)
	fmt.Println("  TX1: コミット (ロック解放)")
	if err := tx1.Commit(); err != nil {
		return fmt.Errorf("TX1 コミットエラー: %w", err)
	}

	// TX2の完了を待つ
	if err := <-done; err != nil {
		return err
	}
	fmt.Println("  TX2: ロック取得成功")
	fmt.Println()

	return nil
}
