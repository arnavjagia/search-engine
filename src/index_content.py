import psycopg2 # type: ignore
import pandas as pd
import string

def normalize_string(input_string: str) -> str:
    translation_table = str.maketrans(string.punctuation + "'", " " * (len(string.punctuation)+1))
    string_without_punc = input_string.translate(translation_table)
    string_without_double_spaces = " ".join(string_without_punc.split())
    return string_without_double_spaces.lower()


def index(url: str, content: str, cursor):
    normalized_content = normalize_string(content)
    query = f"insert into documents values('{url}', '{normalized_content}');"
    cursor.execute(query)

    words = normalized_content.split(" ")

    for word in words:
        query = f"""
            select * from inverted_index 
            where url = '{url}' and word = '{word}'
        """
        cursor.execute(query)

        if cursor.rowcount == 0:
            query = f"insert into inverted_index values('{url}', '{word}', 1);"
        else:
            query = f"""
                update inverted_index set frequency = frequency + 1
                where url = '{url}' and word = '{word}';
            """
        cursor.execute(query)


def bulk_index(pairs: list[(str, str)]):
    conn = psycopg2.connect(
        database="postgres",
        user="postgres", 
        password="", 
        host="localhost", 
        port=5432
    )
    conn.set_session(autocommit=True)

    cursor = conn.cursor()

    for url, content in pairs:
        try:
            index(url, content, cursor)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    df_parquet_path = "data/output.parquet"
    
    df = pd.read_parquet(df_parquet_path)
    df = df.drop_duplicates(subset=["URL"])

    pairs = list(zip(df["URL"].values, df["content"].values))
    bulk_index(pairs)