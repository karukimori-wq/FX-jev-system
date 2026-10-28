# FX-jev-system

Jev AIを選択器として使うFX自動売買システム。**現時点は設計・開発準備段階で、実売買は無効です。**

## 絶対条件
- Jev AIは各ロジックが提示する有限の選択肢から**1つのIDだけ**を返す。理由・予測・数値・信頼度は返さない。
- ロジック別のポジション所有・リスク上限・デモと実取引の分離を維持。
- 価格鮮度・スプレッド・時計差・不整合・UNKNOWN注文は新規取引を安全側に停止。既存ポジション保護は継続。
- 実口座接続・実売買は、テストと管理者承認が完了するまで禁止。

## 設計資料
- [開発計画](docs/IMPLEMENTATION_PLAN.md)
- [仕様追跡表](docs/TRACEABILITY.md)
- [安全原則](docs/SAFETY.md)

## 正本
[Notion｜FX仕様 Q310〜312](https://app.notion.com/p/407e71121fed81b28eacd44d6fbbcb89) / [統合仕様・Codex移行計画](https://app.notion.com/p/407e71121fed81a28a57de5da7df3674)

> Notionの全312問と既存PoCコードの完全照合は未実施。記録された過去のテスト成功は、このリポジトリのテスト成功を意味しない。

## システムPoC保全・境界照合（2026-10-28）

この実装ブランチには既存注文安全PoC **v0.27.0** を `poc/order-safety/` に追加しています。移植先で **75モックテスト成功**。本番・Jev・ブローカー接続は未実装です。

- [移植原本・ハッシュ・再実行証跡](docs/SYSTEM_POC_IMPORT.md)
- [ロジック仕様とシステムの関係・整合性照合](docs/SYSTEM_LOGIC_ALIGNMENT.md)
- [P0: Q307デモ相互干渉の修正](https://github.com/karukimori-wq/FX-jev-system/issues/1)
- [P1: ロジック/Jev・期限・リスク契約の接続](https://github.com/karukimori-wq/FX-jev-system/issues/2)

**確認済みの不足**：デモ隔離によるlive予約阻害、SQLite版の終了日時未適用。現75テスト成功を全312問への適合完了・完全デモ分離と解釈しないでください。上記の初期設計段階の記述はmain登録時の履歴であり、このブランチのコード存在・試験結果は本節と移植記録で更新します。

