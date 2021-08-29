import os, hashlib, json, re, time
import MeCab, ipadic
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials

FIRESTORE_PROJECT_ID = "fulltext-project"
TEXTS_COLLECTION_NAME = "texts"
TERMS_COLLECTION_NAME = "terms"

class FulltextSearch:
    def __init__(self):
        if not firebase_admin._apps:
            cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(cred,{
                'projectId': FIRESTORE_PROJECT_ID,
            })

        self.db = firestore.client()
        self.text_collection = self.db.collection(TEXTS_COLLECTION_NAME)
        self.terms_collection = self.db.collection(TERMS_COLLECTION_NAME)
        self.is_debug = False
        self.read_cnt = 0
        self.update_cnt = 0
        self.tagger = MeCab.Tagger(ipadic.MECAB_ARGS)
        self.tagger.parse('')

    def print_access_count(self):
        if self.is_debug:
            print("read_cnt: ", self.read_cnt, "update_cnt: ", self.update_cnt)

    # 検索用のハッシュを作成する。テキストが長いと検索できないので。
    def __hash_text(self, text:str) -> str:
        # sha256でもいいが、データ量を削減したいのでmd5で。
        hashed_str = hashlib.md5(text.encode('utf-8')).hexdigest()
        return hashed_str

    # text が text_collectionの中にすでに存在するかどうかを確認する
    def __text_exists(self, text:str, doc_id:str = None) -> bool:
        if doc_id is not None:
            doc = self.text_collection.document(doc_id).get()
            return doc.exists
        hash = self.__hash_text(text)
        query_ref = self.text_collection.where("hash", "==", hash)
        self.read_cnt += 1
        docs = query_ref.stream()
        for doc in docs:
            self.read_cnt += 1
            if doc.to_dict()["text"] == text:
                return True
        return False
    
    # doc id をtextに設定しない場合にdoc id を取得したいときに使う
    def get_doc_id_from_text(self, text:str) -> str:
        hash = self.__hash_text(text)
        query_ref = self.text_collection.where("hash", "==", hash)
        self.read_cnt += 1
        docs = query_ref.stream()
        for doc in docs:
            self.read_cnt += 1
            if doc.to_dict()["text"] == text:
                return doc.id
        return ""

    # textをtext_collectionに追加する
    def __add_text(self, text:str, doc_id:str = None, metadata:dict = None) -> str:
        if self.__text_exists(text, doc_id):
            # すでにtextが存在するなら何もしない
            return ""

        # textが存在しないので実際にcollectionにデータを追加する
        hash = self.__hash_text(text)
        body = {"text": text, "hash":hash}
        if metadata is not None:
            for k, v in dict.items():
                body[k] = v

        if doc_id is None:
            _, doc_ref = self.text_collection.add(document_data=body)
        else:
            _, doc_ref = self.text_collection.document(doc_id).set(body)
        doc_id = doc_ref.id
        self.update_cnt += 1
        return doc_id

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

    # 複数件をまとめてfirestoreに入れる
    # text_list: (text, metadata) のリスト
    def insert_text_list(self, text_list:list) -> list:
        terms_dict = {}
        text_doc_ids = []
        for text, metadata in text_list:
            # まずテキストデータを入れる
            text_doc_id = self.__add_text(text, metadata)
            if text_doc_id == "":
                # print("このデータはすでに入っているので飛ばす", text[:30])
                continue
            text_doc_ids.append(text_doc_id)

            # テキストデータは無事入ったのでその後の処理を行う
            terms = self.__wakati_text(text)
            for term in terms:
                if term in [".", ".."] or term.find("/") >= 0:
                    # print("doc_idに {} は使えません".format(term))
                    continue
                if term not in terms_dict:
                    terms_dict[term] = {"term": term, "doc_ids":{text_doc_id:0}, "num_docs": 0}
                if text_doc_id not in terms_dict[term]["doc_ids"]:
                    terms_dict[term]["doc_ids"][text_doc_id] = 0
                
                terms_dict[term]["doc_ids"][text_doc_id] += 1 / len(terms)
                terms_dict[term]["num_docs"] = len(terms_dict[term]["doc_ids"])

        # データを入れる
        for term, item in terms_dict.items():
            item["num_docs"] = firestore.Increment(item["num_docs"])
            self.update_cnt += 1
            self.terms_collection.document(term).set(item, merge=True)
            
        return text_doc_ids

    # テキストをfirestoreに入れて全文検索可能にする
    def insert_text(self, text:str, doc_id:str = None, metadata:dict=None) -> str:
        # まずはテキストデータを入れる
        text_doc_id = self.__add_text(text, doc_id, metadata)
        if text_doc_id == "":
            # すでに入っている場合はここで終了する。TODO 終了しなくてもいいかも
            return ""

        # テキストデータは無事入ったので次は単語を入れる
        terms = self.__wakati_text(text)
        # 単語ごとにデータをまとめてからfirestoreに入れる
        terms_dict = {}
        for term in terms:
            # こういう形にしたい↓
            # {
            #   "term": "単語", 
            #   "doc_ids":{
            #     "document_id" : term_frequency
            #     "8F2KUAonFTYKrplyMhzE" : 0.1,
            #     "0.004807692307692308" : 0.2,
            #     "Ch8Fflim1UmT3xT8ptHi" : 0.3
            #   },
            #   "num_docs": 4 <= 単語が含まれるドキュメントの数
            # }
            if term in [".", ".."] or term.find("/") >= 0:
                # print("doc_idに {} は使えません".format(term))
                continue

            if term not in terms_dict:
                terms_dict[term] = {"term": term, "doc_ids":{text_doc_id:0}, "num_docs": 0}
            if text_doc_id not in terms_dict[term]["doc_ids"]:
                terms_dict[term]["doc_ids"][text_doc_id] = 0
            terms_dict[term]["doc_ids"][text_doc_id] += 1 / len(terms) # term frequency
            terms_dict[term]["num_docs"] = len(terms_dict[term]["doc_ids"])# doc frequecy に該当する値。実はtfidf計算時には使ってない。

        # データを入れる
        for term, item in terms_dict.items():
            item["num_docs"] = firestore.Increment(item["num_docs"])
            self.update_cnt += 1
            self.terms_collection.document(term).set(item, merge=True)
        
        return text_doc_id

    # テキストとそのデータを消す
    def delete(self, text_doc_id:str) -> str:
        # termsを検索して、該当のtermデータからdoc_idを消す
        query = self.terms_collection.where("doc_ids.`{}`".format(text_doc_id), ">=", 0.0)
        self.read_cnt += 1
        docs = query.stream()
        for doc in docs:
            self.read_cnt += 1
            body = {
                "doc_ids.{}".format(text_doc_id): firestore.DELETE_FIELD,
                "num_docs":firestore.Increment(-1)
            }
            term_doc_id = doc.id
            self.terms_collection.document(term_doc_id).update(body)
            self.update_cnt += 1
        # textのデータも消す
        res = self.text_collection.document(text_doc_id).delete()
        self.update_cnt += 1
        self.print_access_count()

        return str(res)

    # 検索を実行する
    def search(self, query_str:str, limit:int=10, should_match_all:bool=True) -> list:
        now = time.time()
        # クエリ文字列を分解する
        query_str = re.sub(r"[ 　]+", " ", query_str)
        query_terms = set()
        for term in query_str.split(" "):
            query_terms = query_terms | set(self.__wakati_text(term))
        query_terms = list(query_terms)

        # 各ワードが含まれるドキュメントのIDを取得する
        query_results = {}
        for term in query_terms:
            query = self.terms_collection.where("term", "==", term)
            docs = query.stream()
            for doc in docs:
                doc_dict = doc.to_dict()
                for text_doc_id in doc_dict["doc_ids"]:
                    if text_doc_id not in query_results:
                        query_results[text_doc_id] = {}
                    tfidf = doc_dict["doc_ids"][text_doc_id] / len(doc_dict["doc_ids"])
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
            if i > limit:
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
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'method' in request.args:
        method = request.args.get('method')
    else:
        return json.dumps({"error": "specify a method name ([insert, insert_text_list, delete, delete_by_text, search])."})

    if method not in ["insert", "insert_text_list", "delete", "delete_by_text", "search"]:
        return json.dumps({"error": "specify a valid method name ([insert, insert_text_list, delete, delete_by_text, search])."})


    text = None
    doc_id = None
    q = None
    text_list = None
    metadata = None
    if request.args and 'text' in request.args:
        text = request.args.get('text')
    if request.args and 'metadata' in request.args:
        metadata_json_str = request.args.get('metadata')
        metadata = json.loads(metadata_json_str) # TODO try-except
    if request.args and 'doc_id' in request.args:
        doc_id = request.args.get('doc_id')
    if request.args and 'q' in request.args:
        q = request.args.get('q')
    if request.args and 'text_list' in request.args:
        text_list_json_str = request.args.get('text_list')
        text_list = json.loads(text_list_json_str)
    
    # メソッドごとに処理を分ける
    fulltext_search = FulltextSearch()
    if method == "insert":
        if text is None:
            return json.dumps({"error": "specify text parameter. "})

        new_doc_id = fulltext_search.insert_text(text, doc_id, metadata)
        if new_doc_id == "":
            return json.dumps({"result": "already exists", "text": text})
        else:
            return json.dumps({"result": "created", "doc_id": new_doc_id})

    if method == "insert_text_list":
        if text_list is None:
            return json.dumps({"error": "specify text_list parameter. "})

        text_doc_ids = fulltext_search.insert_text_list(text_list)
        return json.dumps({"result": "created", "text_doc_ids": text_doc_ids})

    if method == "delete":
        if doc_id is None:
            return json.dumps({"error": "specify doc_id parameter. "})

        res = fulltext_search.delete(doc_id)
        return json.dumps({"result": res})

    if method == "delete_by_text":
        if text is None:
            return json.dumps({"error": "specify text parameter. "})

        text_doc_id = fulltext_search.get_doc_id_from_text(text)
        if text_doc_id == "":
            # データが無い
            return json.dumps({"result": "missing text", "text":text})

        res = fulltext_search.delete(text_doc_id)
        return json.dumps({"result": res})

    if method == "search":
        if q is None:
            return json.dumps({"error": "specify q parameter. "})

        results = fulltext_search.search(q)
        return json.dumps(results)
    

    return json.dumps({"error": "something's wrong."})
