# P0｜模擬イベントの欠番・順不同・再送と持続的な受信監査

## 目的・前提

FX-jev-systemの `codex/fx-foundation-p0` 専用。**実ブローカーへの接続・注文送信・LIVE有効化を一切行わない。** イベントのsequence、stream_id、watermark（「この番号までは存在する」とする申告）は**未認証の模擬入力**であり、実ブローカー側の欠番・実約定時刻・完全な履歴を証明するものではない。

既存の `mock_fill_inbox`（約定実行ID単位）と別に、**輸送イベントの順序番号**を永続化する。ローカル会計確定順の `mock_position_journal` とも別物で、受信順・約定発生時刻・会計記録順は混同しない。

## 新規ファイル・テーブル

- `fxjev/sequenced_feed.py`
  - `MockSequencedFill(stream_id, sequence, notice)`
  - `ingest_mock_sequenced_fill(ledger, envelope)`
  - `observe_mock_watermark(ledger, account_id=..., mode=..., stream_id=..., last_sequence=...)`
- `mock_feed_streams`：`(account_id,mode)` ごとに現在の `stream_id`、最小未受信番号 `next_expected`、申告された最大番号 `high_watermark`、消えない警報 `has_alarm` を保持。
- `mock_feed_messages`：`(account_id,mode,sequence)` 一意の受信記録。stream ID、約定ID、内容SHA-256、受信時刻を保存。
- `mock_feed_alerts`：欠番・番号衝突・不意のストリーム切替・再送約定ID・高水位から分かる未受信イベント・高水位後退・既存の非順序受信経路による迂回試行などを追記監査。

## 取り込みの厳格ルール

1. 最初のstream IDをその口座・modeに固定し、番号1から連番で待つ。別stream IDが来た場合は無断切替を拒否・監査・保留。**ストリーム切替/epochの正規承認は未実装**。
2. 初めて見た番号が `next_expected` より大きい場合は欠番。**同一SQLiteトランザクション**で受信記録、約定台帳、異常監査とアカウント保留を書き込む。
3. `next_expected` 以前の欠番が遅れて届けば、受信表から連続番号を確認してチェックポイントを前進させる。ただし、`has_alarm` を自動で0にせず、該当口座の新規予約・モック再開承認を拒否したままにする。
4. 同じ番号で同じ内容の再送は `duplicate`。新しい約定として再記録しない。同番号で内容が異なれば `conflicting-sequence` を監査して拒否。
5. 別の連番を付けて同じ約定IDを再送する場合も `execution-reused-at-new-sequence` として監査し、注文受付を停止。元の `mock_fill_inbox` の実行ID単位の照合も維持。
6. `high_watermark` は、呼び出し側が「存在する最大番号」と申告した**モック値**。未受信の末尾番号を検出できるが、申告がなければ最後の未到着イベントを予見できない。以前の値より後退した場合も監査し、保留。
7. 一度streamが有効になった口座・modeで、従来の `ingest_mock_fill` を直接呼び出した場合は `unsequenced-bypass` を追記監査し、永続警報を保持。通常のイベント保留が後で消えても、新規予約・モック起動承認を停止する。
8. 会計台帳に対して受信イベントの存在だけで残高の自動減少や保留解除をしない。既存の模擬全量照合が必要であり、**通信番号が連続していても安全証拠の完全性は証明されない**。

## 永続的な安全停止

- `mock_feed_streams.has_alarm=1` がある場合、`Ledger.reserve` は同一トランザクションでその口座・modeの新規予約を拒否する。legacy口座の警報は同じmodeの他の口座にも波及。
- `TradingRuntime.approve_demo` は、該当口座またはlegacyの警報がある限り、モック承認を拒否する。
- `mock_event_holds` にも `__feed__:<stream_id>` 等の保留を残す。**警報を解除するAPIは設けない**。後から欠番が埋まっても再開しない。独立した監査証拠・権限検証付きの保留解除は将来工程。
- 処理は `Ledger._tx` / SQLite `BEGIN IMMEDIATE` の中で、順序監査と約定IDの初回登録・衝突監査をまとめて処理。例外を投げるべき衝突でも、**矛盾の監査と停止をコミットしてから**例外を返す。

## 受入試験

`tests/test_sequenced_feed.py` に模擬受信の自動テストを追加した。主なケースは連番・中間欠番・冒頭欠番・欠番の遅延到着・同番号再送・番号内容変更・別番号で同じ約定ID・ストリーム変更・watermarkの末尾欠番/後退・モード/口座分離・再起動・並列同番号受信・順序なしAPIの迂回・通常保留の削除後も警報による停止維持。

```sh
python -m unittest discover -s tests -v
```

GitHub Actions証跡（最終版を確認して記録）：https://github.com/karukimori-wq/FX-jev-system/actions/runs/41764148441

## 本番化に先立つ未実装条件

- 実ブローカーで一貫して付番されたシーケンス・epoch/stream再起動識別・認証済み署名などがあり、その完全性を独立して検証できること。**業者が単調seqを提供しない場合は異なる方式が必要**。
- WebSocket切断・イベント順不同・欠落・キャンセルとの競合・イベントスナップショット再取得・重複経路・多ホストフェンシングの実ブローカー相当検証。
- API認証済みの注文履歴・全約定ID・個別建玉/ネット建玉の帰属確認、資金・証拠金と損失上限・保護注文。
- 外部署名または不変ストレージへの監査ログ転送、管理者本人認証、手動レビューと警報解除/モック再開の安全な承認機構。
- 旧SQLiteの欠損した履歴を推定で補完しない。設計Q1～312の完全なトレーサビリティは別途必要。

**安全上の固定条件**：Jev AIはロジックが事前定義した候補IDを1つだけ返す。実ブローカー接続・実売買・本番公開・main統合を行わない。
