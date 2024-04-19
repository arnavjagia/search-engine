import time
from math import log
from src.index_content import normalize_string
# from index_content import normalize_string
import psycopg2 # type: ignore


def update_url_scores(old: dict[str, float], new: dict[str, float]):
    for url, score in new.items():
        if url in old:
            old[url] += score
        else:
            old[url] = score
    return old


class SearchEngine:
    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.conn = psycopg2.connect(
            database="postgres",
            user="postgres", 
            password="", 
            host="localhost", 
            port=5432
        )
        self.conn.set_session()
        self.cursor = self.conn.cursor()
        
        self.k1 = k1
        self.b = b

    @property
    def size(self) -> int:
        self.cursor.execute("select count(*) from documents;")
        return self.cursor.fetchone()[0]
    

    @property
    def posts(self) -> list[str]:
        self.cursor.execute("select distinct url from documents;")
        return [t[0] for t in self.cursor.fetchall()]


    @property
    def avdl(self) -> float:
        if not hasattr(self, "_avdl"):
            self.cursor.execute("select avg(length(url)) from documents;")
            self._avdl = float(self.cursor.fetchone()[0])
        return self._avdl


    def idf(self, kw: str) -> float:
        N = self.size
        n_kw = len(self.get_urls(kw))
        return log((N - n_kw + 0.5) / (n_kw + 0.5) + 1)
    

    def bm25(self, kw: str) -> dict[str, float]:
        result = {}
        
        idf_score = self.idf(kw)
        avdl = self.avdl

        for url, freq in self.get_urls(kw).items():
            self.cursor.execute(f"select length(content) from documents where url = '{url}'")
            D = self.cursor.fetchone()[0]

            numerator = freq * (self.k1 + 1)
            denominator = freq + self.k1 * (1 - self.b + self.b * D / avdl)
            result[url] = idf_score * numerator / denominator

        return result
    

    def search(self, query: str) -> dict[str, float]:
        keywords = normalize_string(query).split(" ")
        url_scores: dict[str, float] = {}

        for kw in keywords:
            kw_urls_score = self.bm25(kw)
            url_scores = update_url_scores(url_scores, kw_urls_score)

        # ranked_urls = sorted(url_scores, key=lambda x: url_scores[x], reverse=True)
        return url_scores
    
    
    def get_urls(self, keyword: str) -> dict[str, int]:
        result = {}
        keyword = normalize_string(keyword)

        self.cursor.execute(f"select url, frequency from inverted_index where word = '{keyword}'")
        
        for row in self.cursor.fetchall():
            result[row[0]] = row[1]

        return result


if __name__ == "__main__":    
    while True:
        string = input("\n\nSearch query: ")

        start = time.time()
        
        engine = SearchEngine()
        print(engine.posts)
        results = engine.search(string)

        print(len(results), "results, ", round(time.time() - start, 2), "seconds\n")
        # for result in results[:10]:
        #     print("-", result)
    