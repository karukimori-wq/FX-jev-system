# v0.30 RunLedger

新規呼出元はRunLedger(ledger,run_id)でrunとmodeを固定し、reserve、ingest_entry_fill、filled_position、reserve_close、ingest_close_fillを使用する。元IDをmode/run/用途で名前空間化し、約定取込前に対象注文、決済前に建玉所有者を確認する。外部ブローカーIDは変換しない。

88テスト成功。同じ元IDをdemo/liveで予約→約定→決済し、DB再接続後の再通知も重複計上しない。別run建玉決済と注文誤参照を拒否。

既存低水準APIは互換維持。全呼出元の強制移行、旧台帳移行、認証権限、成績分離は未実装でIssue #1継続。所有者IDは不変という既存台帳モデル前提。期限未適用は別課題。
