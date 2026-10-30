# v0.33 run別終端照合

RunLedger.reconcile_terminal_entry/closeがrun別IDと所有者確認を通して既存の厳密照合を呼ぶ。候補の内部ID・run・建玉IDは書換えない。外部証拠を対象runへ付け替える処理は禁止。外部IDを内部IDへ対応付けるadapter実装は別途必要。

94テスト成功。別runの終端証拠を拒否し予約と注文状態を維持、正しい所有runにだけ適用。旧API強制移行・実証拠の真正性・認証・成績は未完了。モック専用。
