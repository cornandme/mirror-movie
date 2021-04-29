from konlpy.tag import Komoran
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine

class SearchService(object):
    def __init__(self, word_model, rec_dao):
        self.word_model = word_model
        self.rec_dao = rec_dao
        self.komoran = Komoran()

    def search_movie(self, keyword):
        model = self.word_model.fasttext_word_model
        cluster_df = self.word_model.cluster_df
        movie_vectors = self.word_model.movie_vectors

        # get keyword vector
        tokens = self.komoran.morphs(keyword)
        vectors = np.array([model.wv[token] for token in tokens])
        keyword_vector = np.sum(vectors, axis=0)

        # find closest cluster
        cluster_distances = cluster_df['vector'].map(lambda x: cosine(x, keyword_vector))
        target_cluster = cluster_distances.sort_values(ascending=True).index[0]

        # find closest movie in target cluster
        filtered_df = movie_vectors[movie_vectors['cluster'] == target_cluster]
        distances = filtered_df['vector'].map(lambda x: cosine(x, keyword_vector))
        movie_id = filtered_df.assign(distance=distances).sort_values(by='distance').index[0]

        # dont's return rec if the closest movie is too far from keyword
        target_vector = movie_vectors.loc[movie_id]['vector']
        if cosine(target_vector, keyword_vector) > 0.3:
            return []

        # get similar rec
        return self.rec_dao.similar_rec[movie_id]