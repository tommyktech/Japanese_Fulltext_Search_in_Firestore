# Japanese Full-text Search in Firestore (with Cloud Functions)
Google Cloud Platform(GCP)のFirestoreで関連度付きの全文検索機能をお手軽に実現するためのCloud Functionsスクリプト。

Firestoreには全文検索機能が無いため、全文検索が必要な場合はElasticsearch等を使うかFirestore上に全文検索インデックスを自力で構築する必要があります。
このスクリプトは、そのインデックスの構築を行い、Firestore（とCloud Functions）で全文検索を可能にするためのものです。  

## 機能
* 入力テキストを全文検索可能にする
* 検索結果を関連度で並べて出力する

## 検索性能
1クエリあたり約90ミリ秒  
  
計測条件:  
* Cloud Functionsのスクリプト内のsearchメソッドの実行に掛かる時間を計測
* Firestoreに格納したテキストデータは、ライブドアのニュースコーパスから抽出した記事のうち約50万文字分のデータを使用
* 1クエリ中には1~3個のランダムな単語が含まれる
* クエリに使用した単語は、上記で入れたテキストデータから抽出した単語1000個の中からランダムに選択

## 使い方
詳しい使い方はこちら→https://qiita.com/tommyktech/items/3d726ba11b9bc51bd187

* GCPのプロジェクトを用意し、そこでFirestoreを有効にする
* 必要であれば、main.pyの上の方にあるFIRESTORE_PROJECT_NAME等を編集する
* Cloud FunctionsにHTTPをトリガーとする関数を作成する（メモリは2GB以上推奨）
* そこにmain.pyとrequirements.txtをデプロイする
* HTTPのURLにアクセスして、データの登録・検索・削除が可能であることを確認する
  * データの登録例: https://[CloudFunctionsのトリガーURL]?method=index&text=本日は晴天なり
  * 検索: https://[CloudFunctionsのトリガーURL]?method=search&q=本日は晴天なり
  * 削除: https://[CloudFunctionsのトリガーURL]?method=delete&doc_id=[テキストのdoc_id]


## 仕組み
入力テキストはMeCab(+IPADIC)により単語へと分解され、各単語がFirestoreに格納されます（入力テキストも保存されますが単語とは別のコレクションに格納されます）。
格納される形式は基本的に転置インデックス、つまり`単語 => [テキストが保存されているドキュメントのID]`というMap構造のインデックスになります。  
  
検索時にはクエリ文字列をMeCabで単語へと分解し、その単語を含むテキストのドキュメントIDをインデックスから取得してテキストの一覧を表示します。
このときテキストが関連度順に並び替えられますが、その関連度の計算にはTF-IDFを一部簡略化したものを用いています。  
なお、TF-IDFの計算に必要なデータはすべてインデックスに格納されているため、関連度順に並べ替えるためにテキストデータにアクセスする必要がなく、
これにより検索時のFirestoreへのアクセス回数の削減と検索の高速化を図っています。

## 制約と課題
現時点では、各単語のMapデータが1MBを超えてしまうとそれ以上新しいテキストを登録できなくなってしまいます（Firestoreの制約のため）。
この制約については対応できしだいスクリプトを更新します。  

また、テキストデータの登録があまりにも高頻度だとFirestoreの別の制限（1秒に1回以上同一のドキュメントを更新できない）に引っかかってしまうため、
一度に大量のデータを登録する場合は`method=insert_text_list`の利用をおすすめします。

あと、HTTPのAPI仕様がダサいので、REST化を検討中です。
もう一つ、一部にトランザクションも導入したいと思っています。
