# FX-jev-system

Jev AIを選択器として使うFX自動売買システム。**現時点は設計・開発準備段階で、実売買は無効です。**

## 絶対条件
- Jev AIは各ロジックが提示する有限の選択肢から**1つのIDだけ**を返す。理由・予測・数値・信頼度は返さない。
- ロジック別のポジション所有・リスク上限・デモと実取引の分離を維持。
- 価格鮮度・スプレッド・時計差・不整合・UNKNOWN注文は新規取引を安全側に停止。既存ポジション保護は継続。
- 実口座接続・実売買は、テストと管理者承認が完了するまで禁止。

## P0モック安全基盤（Draft PR、実売買禁止）

現在のブランチ `codex/fx-foundation-p0` には、SQLiteによる模擬注文台帳、口座別リスク予約、UNKNOWN注文の照合、モック建玉残高、SIGKILL障害注入、起動時の安全ゲート、遅延・重複した模擬約定の永続イベント台帳、DEMO確定済み注文の追加約定再照合（部分／全決済済みの履歴を含む）、初回エントリー約定ID保存と訂正履歴の連鎖検証、決済との交互更新を再生する会計ジャーナル、欠番・順不同・重複を監査する模擬受信ゲートがあります。

実ブローカーAPI・実発注・損切り処理・本番の承認認証は **未実装** です。以下のコマンドはテスト用SQLiteを安全側に起動するだけで、注文は送信しません。

```sh
python -m fxjev.demo_service --db ./fx-mock.sqlite
python -m unittest discover -s tests -v
```

- [イベント欠番・重複再送・高水位・持続的安全停止](docs/SEQUENCED_FEED_P0.md)
- [会計ジャーナル・交互更新の再生（DEMOモック限定）](docs/ACCOUNTING_JOURNAL_P0.md)
- [初回約定ID・訂正履歴・旧DB保護](docs/EXECUTION_HISTORY_P0.md)
- [決済履歴を含むDEMO増分再照合・監査（本番不可）](docs/LIFECYCLE_RECONCILIATION_P0.md)
- [決済履歴のないDEMO確定注文の再照合](docs/LATE_RECONCILIATION_P0.md)
- [P0イベントの隔離とリスク再評価](docs/EVENT_INBOX_P0.md)
- [P0起動ゲート・安全上の未対応事項](docs/STARTUP_GATE_P0.md)
- [P0障害注入の検証範囲](docs/FAULT_INJECTION_P0.md)
- [Draft PR #4](https://github.com/karukimori-wq/FX-jev-system/pull/4)

## 設計資料
- [開発計画](docs/IMPLEMENTATION_PLAN.md)
- [仕様追跡表](docs/TRACEABILITY.md)
- [安全原則](docs/SAFETY.md)

## 正本
[Notion｜FX仕様 Q310〜312](https://app.notion.com/p/407e71121fed81b28eacd44d6fbbcb89) / [統合仕様・Codex移行計画](https://app.notion.com/p/407e71121fed81a28a57de5da7df3674)

> Notionの全312問と既存PoCコードの完全照合は未実施。記録された過去のテスト成功は、このリポジトリのテスト成功を意味しない。
