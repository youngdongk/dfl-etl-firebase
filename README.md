# dfl-etl-firebase

FirebaseAnalyticsのJSON形式のBigQueryスキーマを、フラットな一行のタブ区切りファイル(TSV)形式に変換するためのパイプライン処理をCloud Dataflowで実装した例です。

Google Cloud PlatformのCloud Dataflowの他に、Cloud Storageも使います。

## 依存ライブラリ
JSONパースのためにjson-simpleを使います。
https://code.google.com/archive/p/json-simple/

ビルドパスにjson-simple-1.1.1.jarを追加してビルドしてください。

## 使い方
https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-eclipse
の記事を参考にして、Eclipse上でDataflowを使えるようにしてください。

Argumentが2つ必要です。

input: BigQueryからエクスポートしたJSONファイル

output: Cloud Storageにエクスポートするファイルのprefix


```
--input=gs:<bucket名>/変換対象のファイル --output=gs://<bucket名>/出力ファイルのprefix
```
