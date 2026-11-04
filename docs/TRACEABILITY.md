# 仕様トレーサビリティ（初期台帳・P0部分更新）

| 領域 | 仕様ID・根拠 | 実装ファイル | テスト | 状態 |
|---|---|---|---|---|
| Jevの選択肢ID契約 | Q1–312横断 | なし | なし | 仕様確定、AI接続未実装 |
| 原子的予約・ロジック単位の数量上限 | Notion確定仕様（全設問照合未完了） | `fxjev/ledger.py` | `tests/test_ledger.py` | モックSQLite実装・CI検証 |
| 注文UNKNOWN・約定・建玉の突合 | Notion Q304–306など | `fxjev/reconciliation.py` | `tests/test_reconciliation.py` | モック証拠のみ検証 |
| 部分決済・全決済後のリスク残数 | Notion Q307–309など | `fxjev/positions.py` | `tests/test_positions.py` | モック証拠のみ検証 |
| 口座ごとの数量上限・UNKNOWN隔離 | 口座分離の詳細設計・P0移行 | `fxjev/ledger.py`、`docs/ACCOUNT_SCOPE_P0.md` | `tests/test_account_scopes.py` | 口座別模擬SQLiteテスト成功 |
| 旧SQLite資産のフェイルクローズ移行 | P0安全基盤 | `fxjev/ledger.py` | `tests/test_migration.py`、`tests/test_account_scopes.py` | テスト済。実データ移行は未実施 |
| 実ブローカーAPI・注文送信・資金リスク | 今後の実装依存 | なし | なし | 未実装 |
| SIGKILL前後のSQLite原子性・未解決予約の保守的復旧 | P0障害復旧・Q304–306ほか | `fxjev/ledger.py`、`docs/FAULT_INJECTION_P0.md` | `tests/test_fault_injection.py` | 強制終了と遅延モックの16テストをCI確認。実ブローカーとの一貫性は未実証 |
| モック約定イベントの重複排除・保存・遅延追加約定の口座隔離 | P0イベント台帳・注文UNKNOWN安全性 | `fxjev/events.py`、`fxjev/ledger.py`、`fxjev/reconciliation.py`、`docs/EVENT_INBOX_P0.md` | `tests/test_mock_events.py` | 25件のモックテスト。実ブローカー証拠未対応 |
| 最終確定済み注文への追加約定と保留訂正 | GitHub Issue #6・確定済み注文の遅延約定 | `fxjev/late_reconciliation.py`、`docs/LATE_RECONCILIATION_P0.md` | `tests/test_late_reconciliation.py` | DEMOモックの増分約定のみ・過去決済なし・24テスト検証。本番証拠/管理者認証/過去決済を含む再照合は未実装 |
| 決済履歴を含むDEMO追加約定の再照合・残建玉再構成 | GitHub Issue #6、P0追加約定 | `fxjev/lifecycle_reconciliation.py`、`fxjev/positions.py`、`docs/LIFECYCLE_RECONCILIATION_P0.md` | `tests/test_lifecycle_reconciliation.py` | 27件モック検証。元決済監査は不変。実ブローカー証拠・管理者認証未実装 |
| 初回エントリー約定ID保存・追加訂正ハッシュ連鎖・旧DB隔離 | P0約定履歴・GitHub Issue #6 | `fxjev/execution_history.py`、`fxjev/reconciliation.py`、`fxjev/late_reconciliation.py`、`fxjev/lifecycle_reconciliation.py`、`fxjev/positions.py`、`docs/EXECUTION_HISTORY_P0.md` | `tests/test_execution_history.py`、`tests/test_lifecycle_reconciliation.py` | 24件追加・CI合計238件。旧DB履歴補完なし。未認証・署名なしのSHA連鎖に限定 |
| DEMO会計処理順のエントリー訂正・決済・再起動リプレイ | P0会計時系列/Issue #6 | `fxjev/accounting_journal.py`、`fxjev/positions.py`、`fxjev/lifecycle_reconciliation.py`、`docs/ACCOUNTING_JOURNAL_P0.md` | `tests/test_accounting_journal.py`、`tests/test_lifecycle_reconciliation.py` | 16件の新規検証・全254テスト成功。実ブローカー発生時刻の真実性は未保証 |
| 模擬イベント受信の欠番・再送・順不同と高水位監査・持続的停止 | P0受信監査/Issue #6 | `fxjev/sequenced_feed.py`、`fxjev/events.py`、`fxjev/ledger.py`、`fxjev/startup.py`、`docs/SEQUENCED_FEED_P0.md` | `tests/test_sequenced_feed.py`、`tests/test_mock_events.py` | 24件の追加検証、全278テスト成功。番号/高水位は未認証モックであり、実ブローカーの完全性保証なし |
| ブローカー送信との原子性・遅延増分約定・複数ホスト | P0安全上の未検証事項 | 未実装 | 未実施 | 本番ブロッカー |
| 起動時ゲート・起動世代の失効・設定変更時の再承認 | Q304–306、Q310–312、GitHub Issue #5 | `fxjev/startup.py`、`fxjev/demo_service.py`、`fxjev/ledger.py`、`docs/STARTUP_GATE_P0.md` | `tests/test_startup_gate.py` | デモ専用モック実装・CI検証。管理者認証と実注文入口は未実装 |
| 管理者承認・監査UI・本番保護 | Q310–312など | 未完成 | なし | 未実装 |
| 設計から実装への移行 | Q310–312 | `docs/IMPLEMENTATION_PLAN.md` | 文書のみ | 初版 |

**注意**: 実装は Draft PR #4 に保全し `main` 未反映。CIのテスト数・成功状況はコミットとワークフロー実行URLに紐付ける。本表の仕様番号は全312問の逐一照合が完了するまで暫定的な参照であり、1問単位の網羅性は保証しない。本番稼働・実ブローカー通信・実売買なし。
