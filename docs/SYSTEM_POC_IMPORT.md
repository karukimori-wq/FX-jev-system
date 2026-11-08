# 注文安全PoC v0.27.0 の保全記録

2026-10-28、システム開発チャットの最新保存版を既存設計リポジトリへ移植。

- 保存版: [fx-order-safety-poc-v0.27.zip](https://chatgpt.com/library?file_id=libfile_ddb042c25a4c8191a756fb2f109022da&account_id=personal)
- 移植先: `poc/order-safety/`。ソース5ファイル、テスト4ファイル、pyprojectと既存READMEのバイト一致を元ZIPと確認。
- 原本のREADME見出しv0.11は旧表記。実際の版はpyprojectのv0.27.0。旧履歴を保全するため原本READMEは変更しない。
- 全移植対象のSHA-256: `poc/order-safety/evidence/import-sha256.json`。
- 移植先で75テスト成功（Python 3.12、0.931秒）。`evidence/import-v0.27-tests.txt`。
- 新たな売買ロジック変更はなし。隔離相互干渉・期限未適用の診断のみ追加。
- SQLite版durable.py/worker.pyが今回の実装対象。engine.pyは初期のインメモリ参照実装で機能同等ではない。
- システム文書版V0.56、ロジック確定仕様版V1.11、ソフトウェア版v0.27.0は別の版番号。

実行:

```bash
cd poc/order-safety
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 evidence/reproduce_alignment_gaps.py
```

公開データは合成モックのみ。DB・鍵・口座情報・実約定データは取り込まない。mainの既存設計文書を起点とする専用ブランチ・Draft PRで保全し、mainマージ・口座接続・実売買は実施しない。
