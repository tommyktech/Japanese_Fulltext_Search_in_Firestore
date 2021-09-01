import os, hashlib, json, re, time, random, string
import MeCab, ipadic
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
from google.api_core import exceptions

FIRESTORE_PROJECT_ID      = "fulltext-project" # Firebaseのプロジェクト名
TEXTS_COLLECTION_NAME     = "texts" # テキストを入れるコレクション。検索結果を表示するときに使う
TERM_LIST_COLLECTION_NAME = "terms_list" # テキストに入ってる単語のリストを入れるコレクション。削除時に使う
TERMS_COLLECTION_NAME     = "terms" # 単語 => テキストのdoc_id のMapを保存するコレクション。検索で使う。

class FulltextIndex:
    def __init__(self):
        if not firebase_admin._apps:
            cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(cred,{
                'projectId': FIRESTORE_PROJECT_ID,
            })

        self.db = firestore.client()
        self.text_collection      = self.db.collection(TEXTS_COLLECTION_NAME)
        self.term_list_collection = self.db.collection(TERM_LIST_COLLECTION_NAME)
        self.terms_collection     = self.db.collection(TERMS_COLLECTION_NAME)
        
        self.is_debug   = False
        self.read_cnt   = 0
        self.update_cnt = 0

        self.tagger = MeCab.Tagger(ipadic.MECAB_ARGS)
        self.tagger.parse('')
        self.delete_timeout = 10


    def print_access_count(self):
        if self.is_debug:
            print("read_cnt: ", self.read_cnt, "update_cnt: ", self.update_cnt)

    # ランダムな document id を作成する関数。Firestoreの自動作成IDは長いのでバイト数の無駄遣い。
    def __create_doc_id(self, len=4):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=len))


    # texts_collection用のdoc_id作成関数。重複があればリトライする
    def __create_texts_collection_doc_id(self, text, len=4):
        loop_cnt = 0
        max_loop_cnt = 10
        while loop_cnt < max_loop_cnt:
            doc_id = self.__create_doc_id()
            doc = self.text_collection.document(doc_id).get()
            if not doc.exists:
                # 存在しないIDが生成できたらreturn
                return doc_id
            loop_cnt += 1
            if loop_cnt >= max_loop_cnt:
                raise Exception("{} collection用のIDが重複しすぎて生成できなかった".format(TEXTS_COLLECTION_NAME))
            print("{} collection用のIDが重複した".format(TEXTS_COLLECTION_NAME))


    # 検索用のハッシュを作成する。テキストが長いと検索できないので。
    def __hash_text(self, text:str) -> str:
        # sha256でもいいが、データ量を削減したいのでmd5で。
        hashed_str = hashlib.md5(text.encode('utf-8')).hexdigest()
        return hashed_str


    # mecabで分かち書き
    def __wakati_text(self, text:str) -> list:
        node = self.tagger.parseToNode(text)
        terms = []

        while node:
            term = node.surface
            pos = node.feature.split(',')[0]
            if pos not in ["助詞", "助動詞", "記号"]:
                if term != "":
                    terms.append(term)
            node = node.next
        return terms


    # テキストデータをコレクションに入れるための構造に変える
    def __build_text_document_data(self, text:str, text_doc_id:str = None, metadata:dict={}):
        if text_doc_id is None:
            # text の doc_id が無いなら生成する。
            text_doc_id = self.__create_texts_collection_doc_id(text)

        if metadata is None:
            metadata = {}

        # text を入れるバッチをセットする
        hash = self.__hash_text(text)
        body = {"text": text, "hash":hash}
        metadata.update(body)
        return text_doc_id, metadata
        

    # 単語データをコレクションに入れるための構造に変える
    def __build_term_document_data(self, text:str, text_doc_id:str):
        terms_dict = {}
        terms = self.__wakati_text(text)
        # 単語コレクションに入れるデータは単語ごとにまとめる
        for term in terms:
            if term in [".", ".."] or term.find("/") >= 0:
                # print("doc_idに {} は使えません".format(term))
                continue

            if term not in terms_dict:
                terms_dict[term] = {"term": term, "doc_ids":{text_doc_id:0}, "num_docs": 0}
            if text_doc_id not in terms_dict[term]["doc_ids"]:
                terms_dict[term]["doc_ids"][text_doc_id] = 0
            terms_dict[term]["doc_ids"][text_doc_id] += 1 / len(terms) # term frequency
            terms_dict[term]["num_docs"] = len(terms_dict[term]["doc_ids"])# doc frequecy に該当する値。実はtfidf計算時には使ってない。

        return terms_dict


    # textsコレクションからデータを取得する
    def get_text_by_id(self, doc_id:str):
        doc = self.text_collection.document(doc_id).get()
        if doc.exists:
            return doc.to_dict()
        
        return None


    # 複数件をまとめてfirestoreに入れる
    # text_list: (text, doc_id, metadata) のリスト
    def batch_index(self, text_list:list) -> list:
        all_terms_dict = {}
        text_doc_ids = []
        batch = self.db.batch()
        for text, text_doc_id, metadata in text_list:
            if text_doc_id is not None:
                # text_doc_id が重複する場合は古い方を削除する
                doc = self.text_collection.document(text_doc_id).get()
                if doc.exists:
                    self.delete(text_doc_id)

            text_doc_id, body = self.__build_text_document_data(text, text_doc_id, metadata)
            text_doc_ids.append(text_doc_id)
            terms_dict = self.__build_term_document_data(text, text_doc_id)
            
            # テキストと単語リストをbatchにセットする
            if len(terms_dict) > 0:
                batch.set(self.text_collection.document(text_doc_id), body)
                batch.set(self.term_list_collection.document(text_doc_id), {"term_list": list(terms_dict.keys())})

            # 単語データはここでまとめる
            all_terms_dict = {**all_terms_dict, **terms_dict}
            

        # 単語データをバッチにセットする
        for term, item in all_terms_dict.items():
            item["num_docs"] = firestore.Increment(item["num_docs"])
            self.update_cnt += 1
            batch.set(self.terms_collection.document(term), item, merge=True)
            
        # コミットする
        batch.commit()
        return text_doc_ids


    # テキストをfirestoreに入れて全文検索可能にする
    def index_text(self, text:str, text_doc_id:str = None, metadata:dict={}) -> str:
        if text_doc_id is not None:
            # doc_id が重複する場合は古い方を削除する
            doc = self.text_collection.document(text_doc_id).get()
            if doc.exists:
                self.delete(text_doc_id)

        # batchでテキストと単語を入れる。まずはテキストのほうをセットする
        batch = self.db.batch()
        text_doc_id, body = self.__build_text_document_data(text, text_doc_id, metadata)
        batch.set(self.text_collection.document(text_doc_id), body)

        # 次は単語をbatchにセットする
        terms_dict = self.__build_term_document_data(text, text_doc_id)

        # 単語データをbatchにセットする
        for term, item in terms_dict.items():
            item["num_docs"] = firestore.Increment(item["num_docs"])
            self.update_cnt += 1
            batch.set(self.terms_collection.document(term), item, merge=True)
        
        # 単語リストをbatchにセットする
        if len(terms_dict) > 0:
            batch.set(self.term_list_collection.document(text_doc_id), {"term_list": list(terms_dict.keys())})
        
        # コミットする
        batch.commit()
        return text_doc_id


    # doc id をtextに設定しない場合にdoc id を取得したいときに使う
    def get_doc_id_from_text(self, text:str) -> str:
        hash = self.__hash_text(text)
        query_ref = self.text_collection.where("hash", "==", hash)
        self.read_cnt += 1
        docs = query_ref.stream()
        doc_ids = []
        for doc in docs:
            self.read_cnt += 1
            if doc.to_dict()["text"] == text:
                doc_ids.append(doc.id)
        return doc_ids


    # テキストとそのデータを消す
    def delete(self, text_doc_id:str) -> bool:
        # 存在チェック＋削除フラグの建立をトランザクションで行う
        transaction = self.db.transaction()
        text_doc_ref = self.text_collection.document(text_doc_id)

        @firestore.transactional
        def update_in_transaction(transaction, text_doc_ref):
            snapshot = text_doc_ref.get(transaction=transaction)
            if not snapshot.exists:
                return False

            doc_dict = snapshot.to_dict()
            if "deleting" in doc_dict.keys():
                # deletingフラグがあり、かつそれが現在から指定秒数以内なら削除しない
                deleting = doc_dict["deleting"]
                sec_diff = time.time() - deleting.timestamp()
                if sec_diff <= self.delete_timeout:
                    # 指定秒数以内なら処理を中断する
                    return False
            # そもそもdeletingフラグが無い、もしくはフラグがタイムアウトしてたらフラグを新しく立てて次に進む
            
            transaction.update(text_doc_ref, {
                u'deleting': firestore.SERVER_TIMESTAMP
            })
            return True

        result = update_in_transaction(transaction, text_doc_ref)
        if result == False:
            # データがない or 削除中フラグが立っているので処理しない
            return False

        # 以下、削除を実行する
        # テキストに含まれる単語を取得して、該当のtermから該当のtext_doc_idを消す
        batch = self.db.batch()
        doc = self.term_list_collection.document(text_doc_id).get()
        for term_doc_id in doc.to_dict()["term_list"]:
            body = {
                "doc_ids.{}".format(text_doc_id): firestore.DELETE_FIELD,
                "num_docs":firestore.Increment(-1)
            }
            
            batch.set(self.terms_collection.document(term_doc_id), body, merge=True)
            # self.terms_collection.document(term_doc_id).update(body)
        # textのデータも消す
        batch.delete(self.text_collection.document(text_doc_id))
        batch.delete(self.term_list_collection.document(text_doc_id))
        batch.commit()

        return True


    # 検索を実行する
    def search(self, query_str:str, size:int=10, should_match_all:bool=True) -> list:
        now = time.time()
        # クエリ文字列を分解する
        query_str = re.sub(r"[ 　]+", " ", query_str)
        query_terms = {}
        for term in query_str.split(" "):
            for t in self.__wakati_text(term):
                query_terms[t] = True
        query_terms = list(query_terms.keys())

        # 各ワードが含まれるドキュメントのIDを取得する
        query_results = {}
        for term in query_terms:
            doc = self.terms_collection.document(term).get()
            if not doc.exists:
                continue
            doc_dict = doc.to_dict()
            for text_doc_id in doc_dict["doc_ids"]:
                term_frequency = doc_dict["doc_ids"][text_doc_id]
                if text_doc_id not in query_results:
                    query_results[text_doc_id] = {}
                tfidf = term_frequency / len(doc_dict["doc_ids"])
                query_results[text_doc_id][term] = tfidf
        # 検索ワードにマッチした結果を取得し、tfidfっぽい値を計算する。
        fully_matched_results = {}
        for text_doc_id, query_result in query_results.items():
            if should_match_all and len(query_result) != len(query_terms):
                # すべての検索ワードにマッチした結果のみを取得するモードの場合は、ヒットした単語数が検索ワード数より少ないものは弾く
                continue
            # 各単語のtfidf値を加算する
            for term, term_tfidf in query_result.items():
                if text_doc_id not in fully_matched_results:
                    fully_matched_results[text_doc_id] = term_tfidf
                else:
                    fully_matched_results[text_doc_id] += term_tfidf

        # tfidf値でソートする
        score_sorted = sorted(fully_matched_results.items(), key=lambda x:x[1], reverse=True)

        # tfidf値の上位limit件を取得して返す
        results = []
        for i, item in enumerate(score_sorted):
            if i > size:
                break
            text_doc_id = item[0]
            score = item[1]
            doc = self.text_collection.document(text_doc_id).get()
            doc_dict = doc.to_dict()
            if doc_dict is None:
                continue
            results.append({"text_doc_id": text_doc_id, "text": doc_dict["text"], "score": score})
        took = str(int((time.time() - now) * 1000)) + " ms"
        return {"total": len(fully_matched_results), "took": took, "hits": results}

def main(request):
    method = None
    request_json = request.get_json(silent=True)
    if request_json and 'method' in request_json:
        method = request_json['method']
    elif request.args and 'method' in request.args:
        method = request.args.get('method')

    if method not in ["get", "index", "batch_index", "delete", "delete_by_text", "search"]:
        return json.dumps({"error": "specify a valid method name ([get index batch_index delete search]).", "request_json": request_json, "request.args": request.args})

    text = None
    doc_id = None
    q = None
    text_list = None
    metadata = {}
    size = 10
    if request.args and 'text' in request.args:
        text = request.args.get('text')
    if request.args and 'metadata' in request.args:
        metadata_json_str = request.args.get('metadata')
        metadata = json.loads(metadata_json_str) # TODO try-except
    if request.args and 'doc_id' in request.args:
        doc_id = request.args.get('doc_id')
    if request.args and 'q' in request.args:
        q = request.args.get('q')
    if request.args and 'size' in request.args:
        size = int(request.args.get('size'))
    if request_json and 'text_list' in request_json:
        text_list = request_json['text_list']
    
    # メソッドごとに処理を分ける
    fulltext_index = FulltextIndex()
    if method == "get":
        if doc_id is None:
            return json.dumps({"error": "specify doc_id parameter. "})

        doc = fulltext_index.get_text_by_id(doc_id)
        if doc is None:
            return json.dumps({"result": "not exists", "doc": doc})
        else:
            return json.dumps({"result": "success", "doc": doc})

    if method == "index":
        if text is None:
            return json.dumps({"error": "specify text parameter. "})
        now = time.time()
        new_doc_id = fulltext_index.index_text(text, doc_id, metadata)
        if new_doc_id == "":
            return json.dumps({"result": "already exists", "text": text})
        else:
            return json.dumps({"result": "created", "doc_id": new_doc_id, "took": time.time() - now})

    if method == "batch_index":
        if text_list is None:
            return json.dumps({"error": "specify text_list parameter. "})

        try  :
            text_doc_ids = fulltext_index.batch_index(text_list)
        except exceptions.InvalidArgument as ex:
            # 400 maximum 500 writes allowed per request
            return json.dumps({"error": "google.api_core.exceptions.InvalidArgument: 400 maximum 500 writes allowed per request"})

        if len(text_doc_ids) > 0:
            return json.dumps({"result": "created", "text_doc_ids": text_doc_ids})
        else:
            return json.dumps({"result": "no documents created"})

    if method == "delete":
        if doc_id is None:
            return json.dumps({"error": "specify doc_id parameter. "})

        res = fulltext_index.delete(doc_id)
        return json.dumps({"result": res})

    if method == "delete_by_text":
        if text is None:
            return json.dumps({"error": "specify text parameter. "})

        text_doc_ids = fulltext_index.get_doc_id_from_text(text)
        if len(text_doc_ids) == 0:
            # データが無い
            return json.dumps({"result": "missing text", "text":text})
        res_arr = []
        for text_doc_id in text_doc_ids:
            res = fulltext_index.delete(text_doc_id)
            res_arr.append(res)
        return json.dumps({"result": res_arr})

    if method == "search":
        if q is None:
            return json.dumps({"error": "specify q parameter. "})

        results = fulltext_index.search(q, size)
        return json.dumps(results)
    

    return json.dumps({"error": "something's wrong."})
