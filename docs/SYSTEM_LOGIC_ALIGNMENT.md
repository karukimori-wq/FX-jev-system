# システム実装とロジック仕様の接続照合（2026-10-28）

## 結論

責任分界は整合するが、機能全体の整合・完成は未達。v0.27.0は鍵不要の注文安全PoCであり、ロジック/Jev実行基盤ではない。75件成功は既存モック試験の結果であり、Notion Q1〜312の全件適合を証明しない。

Notionを確定仕様の正本、GitHubを実装・テスト・未解決事項の正本とする。文書版V1.11とソフトウェア版v0.27.0は別軸。

## 責任分界と接続順序

1. 単一Pythonロジックが発火条件、必要な足・指標、固定選択肢とID、IDから行動への対応を定義。
2. 共通システムが宣言されたデータを収集・計算し、必要時にJevを一度呼ぶ。
3. Jevは有限集合内のID一つだけを返す。理由・信頼度・数値・数量・SL生成を要求しない。
4. 共通システムがIDを検証し、ロジック定義の行動へ変換。未知/複数/空/timeoutは見送り・記録し、代替新規注文をしない。
5. 共通システムが数量、SL、損失、時刻、鮮度等を検査し、検証済み新規意図を注文安全層へ渡す。
6. 注文安全層が所有者、重複、原子的数量予約、送信、UNKNOWN、約定・取消・復旧を扱う。既存建玉管理は新規停止と分ける。

現PoCの `Intent(decision_id, run_id, pair, side, quantity)` は手順5以降の部分的な内部型。Jevの応答型ではなく、これをJevへ生成させない。SL・価格・設定版・接続IDを含む本番契約は未実装。`Side`がBUY/SELLだけでも、見送りはIntentを作らない上流動作なので仕様矛盾ではない。

## 照合表（今回実際に読んだ範囲）

コードパスは `poc/order-safety/` 配下。

| 仕様・根拠 | 現物 | 判定 / 対応 |
|---|---|---|
| Q61〜72: 単一ファイル・ID一択・固定文言・一判断一呼出 | Jevクライアント、戦略ローダー、ID検証なし | 責任分界整合、未実装。Issue #2 |
| Q52 / 08のD015: 同一logic全live runのグロス数量＋run別枠 | `durable.py` `_live_exposure`, `reserve`, `set_logic_limit` | モック試験済。`test_cross_run_parallel_reservations_are_atomic`, `test_stopped_old_run_positions_and_opposite_pairs_count_gross` |
| Q307: demo/liveの注文・建玉・リスク・成績・許可分離 | demo数量除外・DemoBroker経路、run所有は実装 | **部分実装・不整合あり**。隔離・停止・再開・snapshotはmode横断。IDは同一テーブルのglobalキー、成績未実装。Issue #1 |
| Q43 / Q308: 終了日時必須・新規のみ期限停止 | `Run.ends_at`は任意、SQLite runsに期限なし | **未実装**。期限切れ予約受理を再現。Issue #2 |
| Q308: 手動停止後の既存保有管理 | `stop_run`, `reserve_close`, DurableCloseEngine | 所有決済経路はモック試験済。自動SL保護・常時監視は未実装 |
| Q293: 未約定分のみ解放・UNKNOWN保留 | 約定/取消/照合・遅延隔離・修復 | モック試験済。ブローカー最終照合・実API証拠の真正性は未検証 |
| Q265〜270: 接続単位停止・保護UNKNOWN・承認復旧 | 接続ID/保護注文モデルなし。run/logic停止とmock承認のみ | 接続全体停止・本番権限とは同一視しない。未実装 |
| Q292/Q294: 価格鮮度・再計算最終ゲート | 価格/指標/SL/JPYリスクモデルなし | 未実装。数量上限試験だけでは満たさない |
| Q309〜312: 証跡・承認・変更管理 | 75モック試験、今回のGit保全・文書 | 部分実施。本番承認、全312問の1件ごとの追跡表、CI成功は別確認 |

## 再現した具体的不整合

`evidence/reproduce_alignment_gaps.py` を一時DBのみで実行し `evidence/alignment-gaps.json` に保存。外部発注ゼロ。

- demo注文の遅延約定隔離行があると、同じlogicのlive予約が `late fill requires reconciliation` で拒否される。
- `ends_at=2000-01-01T00:00:00Z` のlive runに新規予約できる。

これらは現行の不足を示す診断結果であり受入成功ではない。原本保全のため今回の移植では売買コードを変更していない。既存75テストはこれらの要件を網羅していない。

## 継続順序

1. [Issue #1](https://github.com/karukimori-wq/FX-jev-system/issues/1): Q307のmode別隔離・停止・再開・ID干渉を修正し、live全run数量集約を維持。
2. 決済取消中の全量約定終端状態の既知課題を回帰検証。
3. [Issue #2](https://github.com/karukimori-wq/FX-jev-system/issues/2): 期限必須・永続化とJev/ロジック契約接続、SL/価格/損失/接続別リスク等を仕様ID単位で実装。
4. Notion全312問の重複・矛盾と仕様→コード→試験の全件追跡を完成。今回の境界照合を全件完了と扱わない。

旧Q45の任意理由や旧設計31の任意理由フィールドをAI契約へ復活させない。後続Q63〜72とQ310〜312の「単一IDのみ」を優先する。過去の「P0未発見/v0.11/40件」は履歴とし、現在のv0.27.0/75件とは分ける。

## 参照したNotion

- [統合計画116](https://app.notion.com/p/407e71121fed81a28a57de5da7df3674)、[GitHub登録117](https://app.notion.com/p/407e71121fed81c08684eb4f22653174)
- [契約設計31（設計案）](https://app.notion.com/p/401e71121fed81468969c780296f0950)、[上限・デモ08](https://app.notion.com/p/400e71121fed81a9939dc9a8fe1f5531)
- [Q43〜45](https://app.notion.com/p/401e71121fed817c98e0e6b765ae3888)、[Q52〜54](https://app.notion.com/p/401e71121fed8169a935cca70c2c8682)
- [Q61〜63](https://app.notion.com/p/402e71121fed816a9ab9f1838e84d76f)、[Q64〜66](https://app.notion.com/p/402e71121fed81df94b3f15437fd301c)、[Q67〜69](https://app.notion.com/p/402e71121fed81e686e6d64fbdad007f)、[Q70〜72](https://app.notion.com/p/402e71121fed81aabb4dca1650581d78)
- [Q262〜264](https://app.notion.com/p/405e71121fed8188940ecd3089def9cd)、[Q265〜267](https://app.notion.com/p/405e71121fed81f183cae716f9e44ac7)、[Q268〜270](https://app.notion.com/p/405e71121fed81728e0ff9ea8ac02304)
- [Q292〜294](https://app.notion.com/p/406e71121fed8171a84de77043fee373)、[Q307〜309](https://app.notion.com/p/407e71121fed817da454ff8715a31723)、[Q310〜312](https://app.notion.com/p/407e71121fed81b28eacd44d6fbbcb89)
