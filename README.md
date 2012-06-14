# Haoop MapReduceを使ったappログ解析の実験的なあれこれ

## 使い方
### 事前準備
プロジェクトのビルドにGradleを利用していますので、先にGradleをインストールしておく必要があります。
また、実行環境にはHadoopとMongoDBがインストールされている必要があります。

### ビルド
cloneして出来たプロジェクトのルートで

    gradle build

すると build/libs 以下にjarが出来るので、これを

    hadoop jar hoge.jar　args...

で実行します。
具体的なusage(引数とか)は

    hadoop jar hoge.jar <引数なし>

で実行すると表示されます。

ビルドに必要なライブラリの依存関係はgradleが勝手に解決しますが、生成されたjarにはライブラリは含まれません。
追加で利用したjar(MongoDBのjava用ドライバとかmongo-hadoop連携ライブラリとか)は実行環境のHadoopのlibディレクトリに含めておく必要があります。
