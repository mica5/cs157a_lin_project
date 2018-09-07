#!/usr/bin/env python
import re

import psycopg2
import requests
from bs4 import BeautifulSoup

conn = psycopg2.connect('postgresql://localhost/lin')

cursor = conn.cursor()

cursor.execute("""SELECT;
BEGIN;
DROP SCHEMA IF EXISTS tfidf CASCADE;
CREATE SCHEMA tfidf;

DROP TABLE IF EXISTS documents,document_tokencounts,tokens CASCADE;

CREATE TABLE tfidf.documents (
    did serial primary key,
    title text,
    content text,
    time_added timestamp without time zone default now() not null
);
CREATE TABLE tfidf.tokens (
    tid serial primary key,
    stemword text unique
);
CREATE TABLE document_tokencounts (
    did int references tfidf.documents,
    tid int references tfidf.tokens,
    count int not null default 0,
    constraint document_tokencounts_unique_did_tid unique (did,tid)
);
COMMIT;
""")
conn.commit()

base_website_url = 'http://xanadu.cs.sjsu.edu/~drtylin/classes/cs157A/Project/temp_data/'

resp = requests.get(base_website_url)

soup = BeautifulSoup(resp.content.decode(), 'html.parser')

for i, a in enumerate(soup.find_all('a')):
    resp = requests.get(requests.compat.urljoin(base_website_url, a.get('href')))
    doc = resp.content.decode()

    parts = re.findall('\w+', doc)
    # create the document in the DB
    cursor.execute(
        '''INSERT INTO tfidf.documents (title,content)
            VALUES (%(title)s,%(content)s)
            RETURNING (did)''',
        vars={
            'title': 'doc'+str(i),
            'content': doc,
        }
    )
    conn.commit()
    documentid = cursor.fetchone()[0]

    for word in parts:
        # make sure the token is in the db
        cursor.execute(
            """INSERT INTO tokens (stemword)
                VALUES (%(stemword)s)
                ON conflict (stemword) do
                    UPDATE SET stemword=%(stemword)s
                        WHERE tokens.stemword=%(stemword)s
                returning (tid)""",
            vars={
                'stemword': word.lower(),
            }
        )
        conn.commit()
        tokenid = cursor.fetchone()[0]

        cursor.execute(
            """INSERT INTO document_tokencounts (did,tid,count)
                    VALUES (%(did)s,%(tid)s,1)
                ON conflict (did,tid) DO
                    UPDATE SET count=document_tokencounts.count+1
                    WHERE document_tokencounts.did=%(did)s
                        AND document_tokencounts.tid=%(tid)s""",
            vars={
                'did': documentid,
                'tid': tokenid,
            }
        )
        conn.commit()


cursor.execute('''SELECT
    title
    , count
    , stemword
    FROM documents
    INNER JOIN document_tokencounts using(did)
    INNER JOIN tokens using(tid)
''')
for row in cursor:
    print(row)
conn.close()
