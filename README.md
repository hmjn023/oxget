# oxget

キューを積み上げて使える、Rust製のHTTP(S)ダウンローダーです。端末内で進捗を確認しながら、ダウンロードの一時停止・再開、追加、順序変更ができます。

## インストール

[GitHub Releases](https://github.com/hmjn023/oxget/releases)からLinux向けのmusl静的リンク版を取得できます。Intel / AMD 64-bitは `x86_64-unknown-linux-musl`、ARM 64-bitは `aarch64-unknown-linux-musl` を選び、SHA256SUMSと照合して展開してください。glibcやOpenSSLの共有ライブラリは不要です。

Rustのstableツールチェーンでビルドします。

```sh
cargo install --path . --locked
```

インストール前でも `cargo run --release -- ...` で実行できます。

通常の `cargo build --release` はホスト環境のlibcを使います。配布用muslビルドと通常ビルドは区別してください。

## CIとリリース

GitHub Actionsで、mainへのpush・PR・手動実行時にフォーマット、clippy、全テスト、TUI/IPC操作を確認します。続いてx86_64とARM64のネイティブLinuxランナーでmuslビルド・テストを行い、ELFに動的ローダーや共有ライブラリ依存がないことを検査します。通常の実行ではtar.gzと個別チェックサムをActionsのArtifactsへ保存します。

`Cargo.toml` のversionに合わせた `vVERSION` タグをpushすると、検査に通った両CPU向けのアーカイブとSHA256SUMSをGitHub Releaseへ公開します。アーカイブには実行ファイル・README・LICENSEだけを含めます。ActionsはコミットSHAで固定し、Releaseの作成権限は公開ジョブだけに付与しています。

```sh
git tag -a v0.1.0 -m 'oxget v0.1.0'
git push origin v0.1.0
```

## 使い方

```sh
# URLを指定
oxget 'https://example.com/archive.zip'

# 複数のURLをキューに追加
oxget 'https://example.com/a.zip' 'https://example.com/b.zip'

# URLリスト。位置引数でも -i でも指定可能
oxget urls.txt
oxget -i urls.txt 'https://example.com/extra.zip'

# 保存先、1ファイルあたりの最大接続数、同時に取得するファイル数
oxget urls.txt -d downloads -c 4 -j 2

# 同じ保存先のキューを復元して続行
oxget -d downloads

# 全体停止した状態で開く。p で開始
oxget urls.txt -d downloads --paused

# TUIを使わず、進捗をstderrに表示して終了
oxget urls.txt -d downloads --no-tui
```

URLリストはUTF-8で、1行につき1つのURLを記述します。空行、行頭の空白、`#` で始まるコメント行、ファイル先頭のBOMに対応しています。URL内の `#` はフラグメントとして扱い、`%23` はそのまま保持します。

```text
# 配布ファイル
https://example.com/a.zip
https://example.com/b.tar.gz
```

| キー | 操作 |
| --- | --- |
| `↑` / `↓`、`k` / `j` | 選択を移動 |
| `Space` | 選択した項目を一時停止／再開。失敗項目なら再試行 |
| `K` / `J`（Shift + k / j） | 選択した項目を上／下へ移動 |
| `a` | URLまたはリストファイルのパスを追加。Enterで確定、Escで取消 |
| `p` | 全体を一時停止／再開 |
| `r` | 選択した失敗項目を再試行 |
| `q` / `Esc` / `Ctrl+C` | キューと途中データを保存して終了 |

順序変更は、次に開始するファイルの優先順に反映されます。実行中のファイルを追い越したい場合は、そのファイルをSpaceで一時停止してください。TUIはすべてのダウンロードが終わっても開いたままなので、続けて `a` で追加できます。

## AI・スクリプトからの操作

サブコマンドは端末操作なしで使えます。停止中は保存キューを操作し、ワーカーやTUIが実行中なら、同じ保存先のプロセスへローカルIPCで命令を送ります。どのサブコマンドでも `-d` と `--json` を前後に指定できます。既存の `oxget URL...` 形式も使えます。

```sh
# 登録だけ行い、固定のジョブIDを取得
oxget add -i urls.txt -d downloads --json
printf '%s\n' 'https://example.com/a.zip' | oxget add -i - -d downloads --json

# 常駐ワーカーを起動。操作受付が可能になってから戻る
oxget start --background -d downloads -c 4 -j 2 --json
# 取得・検証がすべて終わったらワーカーも終了
oxget start --background --exit-when-done -d downloads --json

# 状態確認と操作
oxget list -d downloads --state downloading --json
oxget status 12 -d downloads --json
oxget pause 12 -d downloads --json
oxget resume 12 -d downloads --json
oxget move 12 --before 7 -d downloads --json
oxget wait 12 -d downloads --timeout 300 --json

# 保存してワーカー／TUIを終了
oxget stop -d downloads --json
```

| コマンド | 動作 |
| --- | --- |
| `add URL_OR_FILE...`、`add -i FILE` | 登録のみ。`-i -` で標準入力、`--paused` で新規項目を停止状態にする |
| `list [--state STATE]` | 一覧。STATEは `queued`、`downloading`、`verifying`、`paused`、`done`、`failed`、`verification_failed` |
| `status [ID]` | キュー全体または1件の状態・進捗・速度・残り時間・エラー |
| `pause ID...`、`resume ID...` | 指定項目を停止／再開。再実行してもトグルせず同じ状態を保つ |
| `pause --all`、`resume --all` | キュー全体の開始を止める／解放する。個別に停止した項目は停止したまま |
| `move ID --before OTHER`、`--after OTHER`、`--first`、`--last` | 固定IDで優先順を変更。実行中の取得はそのまま継続 |
| `retry ID...`、`retry --failed` | 通信・検証失敗を再キュー。ハッシュ不一致の場合は途中データを破棄して取得し直す |
| `remove ID...` | 対象の転送を止め、キューと途中データを削除。完成ファイルは保持。削除済みIDへの再実行も成功 |
| `start [--background]` | キューが空・全体停止中でも待機する常駐ワーカー。既に実行中ならその状態を返す |
| `stop` | 途中データをフラッシュして保存し、プロセスのロック解放を待つ。停止済みへの再実行も成功 |
| `wait ID... --timeout SECONDS`、`wait --all` | 対象の完了・失敗・タイムアウトを待つ。既定300秒、0は状態を1回確認する。`--all` は開始時点のIDを対象とする |

`add` は同じURLの既存IDを返し、既存項目の状態を変更しません。登録しただけでは取得を開始しませんが、ワーカーが既に動いていれば新規の待機項目を処理します。`resume` と `retry` も停止中のワーカーを自動起動しません。`-c`、`-j`、`-r` はワーカー起動時に指定してください。全体停止は `resume --all` で解除します。

`start` はフォアグラウンドで動作し、`Ctrl+C` または別プロセスからの `stop` まで待機します。JSONで起動結果を受け取る場合は `start --background --json` を使います。バックグラウンドのログは保存先の `.oxget/worker.log` に追記します。通常のTUIが既に開いている場合も、そのTUIが命令を受け付けます。

### SHA-256検証

```sh
# 1つのURLに期待するハッシュを指定（HASHは64桁の16進数）
oxget add 'https://example.com/model.bin' --sha256 HASH -d downloads --json

# 複数URLにはGNU形式のSHA256SUMSを対応付ける
oxget add -i urls.txt --sha256sums SHA256SUMS -d downloads --json
oxget start --background --exit-when-done -d downloads --json
oxget wait --all --timeout 3600 -d downloads --json
```

SHA256SUMSは `64桁ハッシュ  ファイル名` または `64桁ハッシュ *ファイル名` の形式です。空白や日本語を含むファイル名にも対応します。URLのパスをpercent decodeし、リストの相対ファイル名と照合します。指定した全URLに一意の対応が必要で、不足・曖昧な対応・不正なハッシュは登録前にエラーにします。リスト内に取得対象外のファイルがあっても構いません。

取得後、途中ファイルを順に読みながらSHA-256を計算し、一致した場合だけ完成ファイルを公開します。状態は `downloading → verifying → done`、不一致は `verification_failed` です。不一致は自動リトライせず、`retry ID` または `retry --failed` で途中データを捨てて再取得できます。検証中もSpaceや `pause` / `stop` で中断できます。

ジョブのJSONには次を保存し、ワーカー終了・再起動後も確認できます。

```json
{
  "verification": {
    "expected_sha256": "指定したハッシュ",
    "actual_sha256": "実測したハッシュ",
    "result": true
  }
}
```

`result` は一致なら `true`、不一致なら `false`、未検証なら `null` です。ハッシュ未指定の項目は3項目とも `null` です。同じURL・同じハッシュの追加は既存IDを返し、異なるハッシュは `hash_conflict` になります。ハッシュ未設定の待機・停止・失敗項目には後から指定できますが、実行中や完了済みの項目へ新しいハッシュを付けて検証済み扱いにすることはありません。

### 全体進捗と完了時終了

TUI上部と `status` / `list` のJSONの `overall` に、キュー全体のファイル数、完了・失敗・検証中の件数、合計取得バイト数、合計サイズ、取得率、合計速度、ETAを表示します。`status ID` や `list --state ...` でもoverallはキュー全体の集計です。サイズ不明の項目が1つでもあれば `total_bytes`・`percent`・`eta_seconds` は `null` です。停止中・失敗あり・検証中や速度未確定の場合もETAを不明にします。全項目が成功したら100％、ETAは0です。取得率100％でも検証中は未完了です。

`start --exit-when-done` は、全項目が `done` / `failed` / `verification_failed` に到達し、書き込み・検証・結果の保存が終わったら終了します。空キュー・全件完了済みでも正常に起動応答を返して終了します。個別停止や全体停止で待機項目が残っている場合は、再開・削除・`stop` を待ちます。既に動いているワーカーに `start` しても終了モードや接続設定は変更しません。

バックグラウンド起動コマンドの成功は起動受付を表します。取得・検証の最終結果は `wait` または `status` で取得してください。フォアグラウンドの `start --exit-when-done` は最終結果に応じた終了コードを返します。

### ログと通常取得への切り替え理由

バックグラウンドの `worker.log` には起動・終了、ジョブ状態の変更、変更コマンド、エラーを記録します。毎秒のバイト進捗や `status` の照会は記録しません。1ファイルは最大1MiBで、`worker.log.1` ～ `.3` の3世代を残します。詳細な進捗は `status`、TUI、または通常の `--no-tui` 実行で確認できます。

ジョブの `resume_supported` は再開可能なら `true`、通常取得なら `false`、判定前なら `null` です。`transfer_reason` は `no_strong_etag`（強いETagなし）、`range_not_supported`（Range非対応）、`range_ignored_during_transfer`（分割リクエストが無視された）、`empty_resource`（空ファイル）を返します。人向けの理由は `transfer_note`、TUI、通常のstatus出力にも表示します。

### JSON形式と終了コード

`--json` 指定時、stdoutには結果1件だけを出力します。ログはstderrです。引数エラー、存在しないID、通信エラーも同じエンベロープを返します。

```json
{
  "schema_version": 1,
  "ok": true,
  "data": {
    "running": false,
    "pid": null,
    "directory": "/absolute/path/downloads",
    "queue_paused": false,
    "jobs": [],
    "ids": [12],
    "added_ids": [],
    "existing_ids": []
  },
  "error": null
}
```

`list` と `status` のdataには `running`、`pid`、`directory`、`queue_paused`、`jobs` を含みます。変更コマンドには `ids`、`added_ids`、`existing_ids` も含みます。登録・変更の応答の `jobs` は対象の項目だけを含み、`remove` で削除された項目は含みません。`stop` は保存後の全項目を返します。

各ジョブは `id`、1始まりの `position`、`url`、`file_name`、絶対 `path`、`state`、`downloaded_bytes`、`total_bytes`、`speed_bytes_per_second`、`eta_seconds`、`connections`、`error` を持ちます。不明なサイズ・残り時間は `null` です。全体停止中の待機項目は `state: "queued"` のまま、`queue_paused: true` になります。

失敗時は `ok: false` と `error: {"code": "not_found", "message": "Job 999 does not exist"}` を返します。`wait` のタイムアウト・取得失敗には、取得できた最新状態もdataに含みます。IDリスト中に存在しないIDがあれば、`pause`、`resume`、`retry` は全体を変更せずに失敗します。IPCの応答が失われた場合は `status` で状態を確認してください。

| 終了コード | 意味 |
| --- | --- |
| 0 | 成功 |
| 1 | 存在しないID、IPC、保存処理などのエラー |
| 2 | 引数・URL・リスト入力のエラー |
| 3 | 待機・起動・終了のタイムアウト |
| 4 | 対象のダウンロード失敗 |
| 5 | SHA-256検証不一致 |

IPCは127.0.0.1の動的ポートとランダムな認証トークンを使います。接続情報は `.oxget/worker.json` に保存し、正常終了時に削除します。Unixではこのファイルの権限を0600にします。キューの排他ロックを持つプロセスだけが状態を書き込みます。途中で強制終了された場合も、次の起動は保存済みキューと途中データから再開できます。

ハッシュ検証追加前のワーカーが実行中なら、更新後の `oxget stop -d 保存先` で保存・終了してから新しいワーカーを起動してください。旧ワーカーにハッシュ指定が無視されないよう、旧IPCへの変更命令は拒否します。状態確認と停止は旧ワーカーにも送れます。

## 接続数と速度

`-c 4` は、1つのファイルを最大4本のHTTPリクエストに分割して並列取得します。`-j 2` は、最大2つのファイルを同時に取得します。データ取得用リクエストの上限は、おおむね `-c × -j` 本です。HTTP/2では複数リクエストが1本のTCP接続を共有する場合があります。

1本の通信が速度制限や遅延の影響を受けている場合、並列化で速くなることがあります。すでに回線や配信元の帯域を使い切っている場合は増やしても速くなりません。まず `-c 4` 程度から試してください。指定範囲は `-c 1..32`、`-j 1..16` です。

分割取得と途中位置からの再開には、サーバーが実際にRangeリクエストへ応答し、**強いETag**を返す必要があります。非対応・ETagなし・弱いETagのみの場合は、整合性を優先して1本の通常取得へ切り替えます。この場合、停止や終了はできますが、再開時はそのファイルを最初から取得します。`Accept-Ranges` の宣言だけを信用せず、小さなGETリクエストで確認します。HEADを拒否するサーバーでも動作します。

再開前にはETagとサイズを再確認し、変更されていたら途中データを破棄して取得し直します。分割応答の `Content-Range` や長さが不正な場合は、完成ファイルとして保存しません。[HTTP Range / If-Rangeの仕様](https://www.rfc-editor.org/rfc/rfc9110.html#section-14)

## 保存と復元

- キューの順序、個別の停止状態、全体の停止状態を保存先の `.oxget/queue.json` に記録します。
- 途中データは `.oxget/parts/` に保存し、再開時には実ファイルの長さから位置を復元します。
- `q` や `Ctrl+C` で終了した実行中の項目は、次回起動時に自動で続行します。個別に停止した項目は停止したままです。
- 完成したファイルだけを保存先直下に公開します。既存ファイルと名前が重なる場合は `archive.1.zip` のように番号を付け、上書きしません。
- 同じURLを追加しても、キュー内では重複登録しません。URLが同じでもクエリ文字列が違えば別の項目です。
- 保存先ごとに1つのoxgetがキューを管理します。実行中はTUIの `a` または別プロセスの `add` から追加できます。
- 通信エラーは既定で3回再試行します。`-r 0` で無効化、`-r 10` まで指定可能です。接続タイムアウトは15秒、読み取りが進まない場合のタイムアウトは30秒です。
- 端末へ接続されていない場合は自動的にTUIを無効にします。取得失敗の終了コードは4、ハッシュ不一致は5です。失敗項目はTUIの `r` または `retry`、停止項目はSpaceまたは `resume` で再開します。

対象は通常のHTTP(S)ファイル取得です。再帰的なサイト保存や、動画配信プレイリストの結合には対応していません。

## 検証

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
# Unix: 実端末でのキー操作、保存と復元のスモークテスト
cargo build --locked
python3 tests/tui_smoke.py
```

統合テストでは127.0.0.1にHTTPサーバーを起動し、分割取得・再開の整合性に加え、実ワーカープロセスへのIPC、同時追加、実行中項目の削除、停止・再起動、認証、JSON形式、終了コード、stdin入力、タイムアウトを検証します。実端末テストではTUI操作と外部CLI操作の併用も確認します。
