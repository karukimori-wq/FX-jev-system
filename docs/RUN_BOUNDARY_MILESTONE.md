# run境界の限定一区切り v0.36

推奨入口: MockWorker(ledger,broker,mode=...).run_engine(run_id)。RunEngine.scopeがRunLedgerとなる。新規/決済送信、約定、終端照合、停止、新規/決済取消をrun単位に接続済み。起動後の審査必須・mode制約は維持。102テスト成功。

旧MockWorker.submit、DurableEngine、SQLiteLedger低水準APIは互換維持で呼出可能。強制移行・既存ID書換えは未実施で、完全分離完了ではない。旧APIを新たな外部公開入口として増やさない。

次工程: 終了日時のDB永続化と新規予約/送信直前の期限検査。期限到達は新規停止・未約定新規取消のみ、所有建玉の決済/照合を維持。旧DBの期限不明を勝手に補完せず開始不可または明示移行にする。続いて接続別リスク・SL・Jev選択契約・成績。Issue #1/#2継続。mainマージ・実口座接続なし。
