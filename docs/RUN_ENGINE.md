# v0.31.0 RunEngine

RunEngine(ledger,broker,run_id)のsubmit_entryとsubmit_closeがrun別IDを生成し既存DurableEngine/DurableCloseEngineへ接続する。先に予約だけを行うと既存予約扱いで送信されないため、送信用エンジンが予約を一度だけ実行する。決済前に所有者を確認し、既存送信前ゲートも維持。

90テスト成功。同じ元IDのdemo/live送信、再接続後再送防止、外部送信がlive新規/決済各1回のみ、他run決済の送信前拒否を検証。旧低水準APIの強制廃止・旧台帳移行は未実施。Issue #1継続。実ブローカー接続なし。
