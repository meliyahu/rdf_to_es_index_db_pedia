import bz2, os, re
from urllib.parse import unquote
from elasticsearch import helpers
from elasticsearch import Elasticsearch
import sys
import importlib.reload as reload

reload(sys)

sys.setdefaultencoding('utf8')

# Download of data http://downloads.dbpedia.org/2016-04/core-i18n/en/page_links_en.ttl.bz2
filename = "/Users/mosheh/git/mosheh-repo/ontology/page_links_en.ttl.bz2"
indexName = "dbpedialinks"
docTypeName = "article"

linePattern = re.compile(r'<http://dbpedia.org/resource/([^>]*)> <[^>]*> <http://dbpedia.org/resource/([^>]*)>.*',
                         re.MULTILINE | re.DOTALL)

es = Elasticsearch()

print("Wiping any existing index...")
es.indices.delete(index=indexName, ignore=[400, 404])
indexSettings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        docTypeName: {
            "properties": {
                "subject": {
                    "type": "keyword",
                    "fields": {
                        "text": {
                            "type": "text"
                        }
                    }
                },
                "linked_categories": {
                    "type": "keyword",
                    "fields": {
                        "text": {
                            "type": "text"
                        }
                    }
                },
                "linked_subjects": {
                    "type": "keyword",
                    "fields": {
                        "text": {
                            "type": "text"
                        }
                    }
                }
            }
        }
    }
}
es.indices.create(index=indexName, body=indexSettings)

actions = []
rowNum = 0
lastSubject = ""
article = {}
numLinks = 0
numOrigLinks = 0


def addLink(article, subject):
    # Use a separate field for category topics
    if subject.startswith("Category:"):
        article["linked_categories"].append(subject[len("Category:"):])
    else:
        article["linked_subjects"].append(subject)


def newArticle(subject):
    article = {}
    article["subject"] = subject
    article["linked_subjects"] = []
    article["linked_categories"] = []
    addLink(article, subject)
    return article


with os.popen('bzip2 -cd ' + filename) as file:
    for line in file:
        # m = linePattern.match(unicode(line))
        m = linePattern.match(line.decode('UTF-8'))
        if m:
            # Lines consist of [from_article_name] [to_article_name]
            # and are sorted by from_article_name so all related items
            # to from_article_name appear in contiguous lines.
            subject = unquote(m.group(1)).replace('_', ' ')
            linkedSubject = unquote(m.group(2)).replace('_', ' ')

            if rowNum == 0:
                article = newArticle(subject)
                lastSubject = subject
                numLinks = 0
                numOrigLinks = 0
            if subject != lastSubject:
                if len(article["linked_subjects"]) > 1:
                    article["numOrigLinks"] = numOrigLinks
                    article["numLinks"] = numLinks
                    action = {
                        "_index": indexName,
                        '_op_type': 'index',
                        "_type": docTypeName,
                        "_source": article
                    }
                    actions.append(action)
                    # Flush bulk indexing action if necessary
                    if len(actions) >= 5000:
                        helpers.bulk(es, actions)
                        ## TO check for failures and take appropriate action
                        del actions[0:len(actions)]
                # Set up a new doc
                article = newArticle(subject)
                lastSubject = subject
                numLinks = 0
                numOrigLinks = 0

            # Don't want too many outbound links in a single article - slows down things like
            # signif terms and links become tenuous so truncate to max 500
            if len(article["linked_subjects"]) < 500:
                addLink(article, linkedSubject)
                numLinks += 1
            numOrigLinks += 1

            rowNum += 1

            if rowNum % 100000 == 0:
                print(rowNum, subject, linkedSubject)
