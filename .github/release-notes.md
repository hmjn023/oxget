Rust製の再開可能なHTTP(S)ダウンローダーです。

- URL・URLリストを永続キューへ追加
- TUIで進捗表示、一時停止・再開・順序変更
- バックグラウンド実行、JSONサブコマンド、ローカルIPC
- SHA-256・SHA256SUMS検証、不一致時の完成ファイル公開防止
- 全体進捗・ETA、完了時終了、ログローテーション

## Linux binaries

対象CPUに合ったtar.gzをダウンロードしてください。

- `x86_64-unknown-linux-musl`: Intel / AMD 64-bit
- `aarch64-unknown-linux-musl`: ARM 64-bit

どちらもmuslを静的リンクし、glibc・OpenSSLなどの共有ライブラリを必要としません。ELFに動的ローダー・共有ライブラリ依存がないことをCIで検査しています。Linuxカーネルと、DNS解決に必要なOSのネットワーク設定は必要です。

SHA256SUMSと照合してから展開してください。

```sh
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf oxget-VERSION-TARGET.tar.gz
./oxget-VERSION-TARGET/oxget --help
```

使い方とJSON・終了コードの仕様はREADMEに記載しています。更新前のワーカーが動いている場合は、`oxget stop -d 保存先`で停止してから新しい実行ファイルで起動してください。
