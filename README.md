# dfl-etl-firebase

http://ex-ture.com/blog/2018/02/10/dataflow-to-faltten-firebase-schema/

FirebaseAnalyticsのJSON形式のBigQueryスキーマを、フラットな一行のタブ区切りファイル(TSV)形式に変換するためのパイプライン処理をCloud Dataflowで実装した例です。

Google Cloud PlatformのCloud Dataflowの他に、Cloud Storageも使います。

## 依存ライブラリ
JSONパースのためにjson-simpleを使います。
https://code.google.com/archive/p/json-simple/

pom.xmlにdependencyを追加してください。

```
<dependency>
  <groupId>com.googlecode.json-simple</groupId>
  <artifactId>json-simple</artifactId>
  <version>1.1.1</version>
</dependency>
```

## 使い方
https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-eclipse
の記事を参考にして、Eclipse上でDataflowを使えるようにしてください。

Argumentが2つ必要です。

input: BigQueryからエクスポートしたJSONファイル

output: Cloud Storageにエクスポートするファイルのprefix


```
--input=gs:<bucket名>/変換対象のファイル --output=gs://<bucket名>/出力ファイルのprefix
```

コマンドラインで実行したい場合はmavenで実行します。

```
$ mvn compile exec:java \
    -Dexec.mainClass=jp.exture.dataflowdemo.FirebaseJsonConvert \
    -Dexec.args="--project=<project名> \
    --stagingLocation=gs://<バケット名>/<ステージングフォルダ>/ \
    --input=gs://<バケット名>/20160607.json \
    --output=gs://<バケット名>/flat20160607 \
    --runner=DataflowRunner"
```
