"""
Implements Model 2.

Scores a set of queries using a provided Lucene index.

Output:
    Run file with scores for the provided queries

@author: Tino Hakim Lazreg
"""

from __future__ import division

import math

from nordlys.retrieval.index_cache import IndexCache
from nordlys.retrieval.lucene_tools import Lucene
from nordlys.retrieval.results import RetrievalResults
from nordlys.retrieval.scorer import Scorer


class Model2(object):
    """ Model2 scorer """

    FREEBASE_URL = "<http://rdf.freebase.com/ns/entity_id>"
    SCORER_DEBUG = False

    def __init__(self, config):
        # TODO: Set config parameters like in retrieval.py
        self.config = config
        self.queries = []
        self.lucene = IndexCache(self.config['index_dir'])

    def _load_queries(self):
        """
        Loads queries from file, and appends the queries to a list
        """
        query_file = open(self.config['query_file'])
        for query in query_file:
            self.queries.append(query.split("\t"))

    def _open_index(self):
        """
        Opens Lucene IndexSearcher
        """
        self.lucene.open_searcher()

    def _close_index(self):
        """
        Closes Lucene IndexReader
        """
        self.lucene.close_reader()

    def _first_pass_scoring(self, lucene, query):
        """
        Returns first-pass scoring of documents using lucene

        :param lucene: Lucene object
        :param query: Preprocessed query
        :return: RetrievalResults object with doc_id and p(q|d)
        """
        print "\t First pass scoring... "
        results = lucene.score_query(query, field_content=self.config['first_pass_field'],
                                     field_id=self.config['field_id'],
                                     num_docs=self.config['first_pass_num_docs'])
        print results.num_docs()
        print "\t First pass scoring done"
        return results

    @staticmethod
    def _second_pass_scoring(res_first_pass, scorer):
        """
        Return second-pass scoring of documents.
        :param res_first_pass: First pass RetrievalResults object
        :param scorer: ScorerLM object
        :return: RetrievalResults object with doc_id and p(q|d)
        """
        print "\tSecond pass scoring... "
        results = RetrievalResults()
        for doc_id, orig_score in res_first_pass.get_scores_sorted():
            doc_id_int = res_first_pass.get_doc_id_int(doc_id)
            score = scorer.score_doc(doc_id, doc_id_int)
            results.append(doc_id, score)
        print "Second pass scoring done"
        return results

    @staticmethod
    def write_trec_format(query_id, run_id, out, p_q_e, max_rank=100):
        """Outputs results in TREC format

        :param query_id:
        :param run_id:
        :param out: Opened output file
        :param p_q_e: p(q|e) probabilities for the given query
        :param max_rank:
        """
        rank = 1
        # Sort p_q_e_iteritems() by score
        for entity_query_id, score in sorted(p_q_e.iteritems(), key=lambda (k, v): (v, k), reverse=True):
            entity_id, q_id = entity_query_id
            # Need to transform the entity_id to the correct format as in qrels
            entity_id = entity_id[1:].replace("_", ".", 1)
            entity_id = Model2.FREEBASE_URL.replace("entity_id", entity_id)
            if rank <= max_rank:
                out.write(
                        query_id + "\tQ0\t" + entity_id + "\t" + str(rank) + "\t" + str(score) + "\t" + str(
                            run_id) + "\n")
            rank += 1

    def retrieve_entities(self, lucene_doc_id, field, term_freq):
        """ 
        Retrieves all entities associated with a document

        """
        doc_term_vector = self.lucene.get_doc_termvector(lucene_doc_id, field)
        entities = []
        num_entity_mentions = 0
        for term, termenum in doc_term_vector:
            if term.startswith("_m_"):
                entities.append(term)
                num_entity_mentions += term_freq[term]
        if self.SCORER_DEBUG:
            print "\t\t num_entities =" + str(num_entity_mentions)
        return entities, num_entity_mentions

    def get_idf(self, entity, field):
        """
        Returns idf for given entity and field in index

        """
        num_docs = self.lucene.get_doc_count(field)
        doc_freq = self.lucene.get_doc_freq(entity, field)
        if self.SCORER_DEBUG:
            print "\t\t num_docs=" + str(num_docs) + "\t doc_freq=" + str(doc_freq)
        return math.log(num_docs / doc_freq)

    def get_p_e_d(self, doc_id):
        """
        Returns the p(e|d) probability of the given document

        :param doc_id: WARC-TREC-ID of the document
        :return: RetrievalResults object with p(e|d) scores and entity_ids
        """
        field = "contents_annotated"
        p_e_d = RetrievalResults()
        lucene_doc_id = self.lucene.get_lucene_document_id(doc_id)
        term_freq = self.lucene.get_doc_termfreqs(lucene_doc_id, field)
        if self.SCORER_DEBUG:
            print "\t\t doc_id: " + str(doc_id)
        entities, num_entity_mentions = self.retrieve_entities(lucene_doc_id, field, term_freq)
        # Calculate p_e_d for each entity associated with the document
        for entity in entities:
            tf_entity = term_freq[entity] / num_entity_mentions
            # Normalize the score by using IDF
            idf = self.get_idf(entity, field)
            if self.SCORER_DEBUG:
                print "\t\t term_freq= " + str(term_freq[entity]) + "\tnum_entities= " + str(num_entity_mentions)
                print "\t\t association_score= " + str(tf_entity)
                print "\t\t math.log(num_docs/doc_freq)=" + str(idf)
            # Final score
            p_e_d_score = tf_entity * idf
            p_e_d.append(entity, p_e_d_score)
            if self.SCORER_DEBUG:
                print "\t\t p(e|d)= " + str(p_e_d_score)
        return p_e_d

    def get_p_q_d(self, query):
        # Preprocess query
        """
        Returns the p(q|d) probabilities for the query

        """
        query = Lucene.preprocess(query)
        # score collection, to determine a set of relevant documents.
        res_first_pass = self._first_pass_scoring(self.lucene, query)
        # Use LM from scorer.py to calculate p(q|d)
        scorer = Scorer.get_scorer(self.config['model'], self.lucene, query, self.config)
        p_q_d = self._second_pass_scoring(res_first_pass, scorer)
        return p_q_d

    def get_p_q_e(self, q_id, query):
        """
        Return p(q|e) probabilities for the documents in p(q|d)

        """
        p_q_e_all = {}
        # get p(q|d) probs for top n documents
        print "scoring [" + q_id + "] " + query
        p_q_d_all = self.get_p_q_d(query)   

        for doc_id, p_q_d in p_q_d_all.get_scores_sorted():
            p_e_d = self.get_p_e_d(doc_id)
            p_q_d = math.exp(p_q_d)

            if self.SCORER_DEBUG:
                print "\t\t Doc: " + str(doc_id) + "\t math.exp[p(q|d)]=" + str(p_q_d)

            for entity_id, p_e_d in p_e_d.get_scores_sorted():
                if (entity_id, q_id) not in p_q_e_all:
                    p_q_e_all[entity_id, q_id] = 0
                p_q_e_all[entity_id, q_id] += p_q_d * p_e_d

                if self.SCORER_DEBUG:
                    print "\t\t Entity: " + str(entity_id) + "\t p(e|d)=" + str(p_e_d)
                    print "\t\tp(q|e)+= " + str(p_q_e_all[entity_id, q_id])

        # Take log of all scores in p(q|e)
        p_q_e_all.update({k: math.log(v) for k, v in p_q_e_all.items()})

        return p_q_e_all

    def score_all(self):
        """
        Scores all the given queries for the given index, using Model 2

        """
        self._open_index()
        self._load_queries()
        out = open(self.config['output_file'], "w")
        # for each query
        for q_id, query in self.queries:            
            p_q_e_all = self.get_p_q_e(q_id, query)
            # Write p_q_e for query to output_file
            self.write_trec_format(q_id, self.config['run_id'], out, p_q_e_all, self.config['num_docs'])
            print "Scores computed for: " + q_id
            # Clear p_q_e after writing to file
            p_q_e_all.clear()
        # Close output file
        out.close()


def main():
    # TODO: Read config from json
    config = {'index_dir': '/home/tinohl/tino-thesis/output/ClueWeb12_merged_index',
              'field_id': Lucene.FIELDNAME_ID,
              'smoothing_method': "dirichlet",
              'model': "lm",
              #'smoothing_param': 0.1,
              'first_pass_field': Lucene.FIELDNAME_CONTENTS,
              'first_pass_num_docs': 1000,
              'num_docs': 100,
              'run_id': 1,
              'query_file': '/home/tinohl/tino-thesis/data/queries.txt',
              'output_file': '/home/tinohl/tino-thesis/output/new_runs/refactor_test.txt',
              'ann_dir': '/hdd2/export/nordlys/data/ClueWeb12-FACC1'}

    model2 = Model2(config)
    model2.score_all()


if __name__ == '__main__':
    main()
