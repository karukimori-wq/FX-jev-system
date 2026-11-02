# P0 起動時安全ゲート・デモ実行ルート

> **安全境界**：本機能は、ローカルSQLiteとブローカー非接続のモック専用です。実FX口座・取引注文・本番環境・管理者認証は未対応。実売買は一切できません。

## 実行入口

デモ用起動コマンド：

```sh
python -m fxjev.demo_service --db ./fx-mock.sqlite
```

起動時に `TradingRuntime(db_path)` が必ず呼ばれ、SQLite上で以下を原子的に実施します。

1. `runtime_control` の起動世代をインクリメント
2. 登録済みのすべての注文受付ゲートをBLOCKEDに変更し、旧実行世代のトークンを無効化
3. 保留中の `RESERVED` 注文を `UNKNOWN` へ変更し、**既存リスク予約を保持**
4. `startup_recovery_audit` と `runtime_gate_audit` に記録

起動直後はデモ取引を含めた**全注文の受付が停止**しています。コマンド自体は注文や決済を送信しません。

## 起動後にデモモックを許可する手順（テストコード内のみ）

- `Ledger.configure_account("A", Mode.DEMO, 10)` と `Ledger.configure("alpha", Mode.DEMO, 7, account_id="A")` でモック数量上限を登録
- `runtime.approve_demo("A",operator_id="mock-operator",review_ref="demo-check",expected_account_version=...)` を呼ぶ
- `runtime.reserve_demo(...)` でモック予約を作る

**重要**： `operator_id` と `review_ref` は監査用に呼び出し側から渡す「文字列」であり、本人確認された管理者承認ではありません。実ブローカーの注文履歴・約定履歴・建玉の真正性確認も行いません。現在は `approve_live` が常に例外を送出し、実取引を許可できません。

## 注文予約のフェンシング

最初の `TradingRuntime` 起動後はSQLiteに `runtime_control` が永続化されます。それ以降、**従来の `Ledger.reserve` を直接呼んでも**、同一トランザクション内の起動世代・ゲート状態・ランダムなモック予約トークン・承認した口座設定バージョンの照合が通らなければ拒否します。

別のランタイムが起動すれば旧世代のトークンは無効です。別プロセスから古いセッションのトークンで予約できません。

口座設定・ロジック設定の変更、UNKNOWNへの移行、明示的な `recover_unresolved_intents()` でも該当口座のゲートは失効します。失効後は、モック照合と新しい `approve_demo` を経なければ再開しません。

## 対象範囲とテスト

- `fxjev/startup.py`: 起動世代・ゲート・監査・デモ承認・予約ルート
- `fxjev/demo_service.py`: 実行時に必ずBLOCKED状態で起動するデモ用CLI
- `fxjev/ledger.py`: アトミック予約時のゲート検証と設定変更の失効
- `tests/test_startup_gate.py`: 起動／再起動／実行世代の無効化／設定変更／UNKNOWN／デモ分離／旧データ／CLI起動

検証コマンド：
```sh
python -m unittest discover -s tests -v
```

GitHub Actions (Linux) にて **138 tests / OK**：
https://github.com/karukimori-wq/FX-jev-system/actions/runs/41534782310

## 本番利用へのブロッカー（未実装）

1. **外部ブローカー接続・注文照合・建玉保護が一切ない。** モックの完全性フラグや監査ラベルを本番の承認として信用しない。
2. 本ゲートを通らない旧 `Ledger` APIは、最初の `TradingRuntime` が呼ばれる前のDBで引き続き使用できる。プロトタイプの互換性確保のため残しているもので、本番の注文入口として使用禁止。本番採用時には唯一の発注入口を強制し、DBの直接呼出しを防ぐ境界が必要。
3. リスク予約と実ブローカー送信間の原子性、送信結果UNKNOWN、遅延増分約定、孤立建玉の照合は未完成。
4. 管理者の本人認証、承認権限、変更後の再承認、監査証跡の真正性保証が未完成。デモ承認の監査文字列は本人認証ではない。
5. 同一SQLite以外の分散プロセス／複数ホストのフェンシング、永続ストレージ障害・部分ネットワーク障害の検証は未実施。
6. FX実運用に必要な口座認証・損失上限・ポジション保護処理・段階的LIVE有効化は未実装。

## 運用・変更方針

- GitHub Draft PR #4に保全。main反映・本番公開・実FX口座接続・実売買は禁止（別途明示承認が必要）。
- Jev AIはロジックが定義した候補IDから1つ選ぶのみで、理由・数値・信頼度などは返さない。
- 次のP0は **ブローカー非接続のイベント受信台帳と、追加約定を過小評価しないフェイルクローズ照合のモック検証**。
