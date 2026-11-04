# P0｜受信連番開始前の旧履歴隔離・読み取り専用の欠番診断

## 対象と安全境界

`codex/fx-foundation-p0` のSQLiteデモモック。ブローカーAPIの認証済み配信ではなく、`stream_id`、`sequence`、`high_watermark`はいずれも呼び出し側が指定する模擬値。**この結果はブローカーからの全イベント受信を証明しない**。実口座接続・実売買は行わない。

前段の [SEQUENCED_FEED_P0.md](SEQUENCED_FEED_P0.md) には、すでに欠番・順不同・再送・番号衝突の監査と持続的警報がある。今回は移行の安全性と診断のみ追加した。

## 旧イベントから連番受信へ切替えるときの隔離

- `fxjev/sequenced_feed.py` の初回ストリーム作成時、同じaccount/modeについて `mock_fill_inbox` に**既存の非連番約定が1件でもある**ことをチェック。
- 存在する場合、`mock_feed_alerts` に `preexisting-unsequenced-history`、`mock_feed_streams.has_alarm=1`、`mock_event_holds` に`__feed__:<stream_id>`の保留を**同じSQLiteトランザクションで**記録。
- ストリーム最初のイベント受信だけでなく、`observe_mock_watermark()` の最初の呼出しでも検出する。
- 最初の受信が連番1であっても、**過去に連番がないイベントが存在した事実を消さない**。旧約定に架空のsequenceを割り振らない。
- 該当口座/モードの新規予約とDEMO再開の承認を停止。別口座・別モードには不要な停止を広げない。
- 警報は手動で普通の`mock_event_holds`を消しても残り、再起動しても保持される。**警報解除APIは実装しない**。

## 読み取り専用の検査

`fxjev/feed_inspection.py` の `inspect_mock_feed(ledger, account_id=..., mode=..., max_ranges=128)` を利用する。

- `MockFeedInspection`：stream ID、次に期待する番号、高水位、観測最大sequence、受信メッセージ数、未受信番号合計、`missing_ranges`、一覧省略フラグ、チェックポイント整合、各メッセージのstream IDの一致、警報有無、警報件数、イベント保留件数。
- 欠番範囲は、受信済みの最小/最大間のギャップだけでなく、**申告高水位が大きい場合に限り末尾の欠番も表示**する。
- 高水位の申告がない場合は、未来の未到達イベント数を推定しない。欠番0件は配信の完全性や新規注文の承認を意味しない。
- `max_ranges`は1～1024。表示数を省略しても`missing_total`の総数を変えず、`ranges_truncated=True`とする。
- 未初期化のストリームを「安全・警報なし」として返さず、`SafetyError`で拒否。
- SQLiteで`PRAGMA query_only=ON`を使用し、読み取り専用でスナップショットを収集する。**警報を解除せず、ゲートや注文状態を変更しない**。
- 保存済みチェックポイントに欠落や変更があった場合は`checkpoint_consistent=False`を返す。受信メッセージのstream IDが現在のstream IDと一致しなければ`message_stream_consistent=False`を返す。

### ローカル用サンプル

```python
from fxjev.ledger import Ledger, Mode
from fxjev.feed_inspection import inspect_mock_feed

ledger = Ledger("mock.sqlite")
audit = inspect_mock_feed(ledger, account_id="A", mode=Mode.DEMO)
print(audit.missing_ranges, audit.missing_total, audit.has_alarm)
```

`Ledger(...)`はローカルSQLiteを扱うもの。これはレポートの例であり、取引の開始・停止解除・外部API通信は行わない。

## 検証

`tests/test_feed_inspection.py` に18件を追加。

- 正常連続、欠番の後追い補完、複数の欠番範囲、申告高水位を使った未受信末尾、範囲表示数制限、未初期化/空の高水位
- 初回受信前/初回高水位前に存在した非連番イベントの持続的隔離
- アカウント/モード別の分離、保留レコード削除後でも残る警報とDEMO再開拒否
- 再起動後の読取、診断がDBを書き換えないこと、チェックポイント・stream ID改変検知、入力検証

既存278件＋追加18件＝**296 tests / OK**。
GitHub Actions：https://github.com/karukimori-wq/FX-jev-system/actions/runs/41806041805

## 重要な未完了事項

1. 真のブローカーが連番/高水位を提供していない、または断続的にしか提供しない場合、こちらの模擬連番だけでは本当のイベント欠落を識別できない。契約に合った外部証拠方式を別途設計する必要がある。
2. 今回の警報は持続的停止のみ。真の管理者の本人認証、外部原本の再取得、真正性・完全性を検証したうえで警報解除/段階的復旧を行う処理はない。
3. 読み取り専用診断のフラグは**診断用**。現行の予約ゲートは永続警報を確認する。高度なDB破損検査や署名付き外部監査との突合もまだない。
4. 警報のSHA-256、デモ証拠およびreviewラベルは未認証。実口座・実FX注文の安全を保証しない。
5. 全312問の仕様追跡、本番向け資金・証拠金・ロジック/通貨/方向別リスク、多ホスト通信障害、実保護注文は未実装。

**公開方針**：Draft PR #4だけに保全し、mainマージ、ブローカー接続、本番公開、実売買は別途明示承認まで行わない。Jev AIはロジックに提示された候補IDの1つだけを返す。
