import itertools

from konlpy.tag import Komoran
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine

class SearchService(object):
    def __init__(self, word_model, rec_dao, search_dao, movie_info_dao):
        self.word_model = word_model
        self.rec_dao = rec_dao
        self.search_dao = search_dao
        self.movie_info_dao = movie_info_dao
        self.komoran = Komoran()
        self.flatten = itertools.chain.from_iterable


    def search(self, keyword):
        result = self.search_hash(keyword)
        
        # 벡터 검색으로 보충
        if len(result['movies']) < 100:
            movies = self.search_movie_by_vector(keyword)
            result['movies'] = self.get_unique_ordered_list(result['movies'] + movies)[:100]
        else:
            result['movies'] = result['movies'][:100]
        
        '''
        if len(result['similar_words']) < 15:
            keywords = self.get_similar_words_by_vector(keyword)
            result['similar_words'] = self.get_unique_ordered_list(result['similar_words'] + keywords)[:15]
        '''

        # 추천 결과에 제목 붙이기
        movies_df = pd.DataFrame(data={'movie_id': result['movies']})
        merged_df = pd.merge(
            movies_df, 
            self.movie_info_dao.movie_info[['title_kor']], 
            left_on='movie_id',
            right_index=True,
            validate='one_to_one'
        )
        result['movies'] = merged_df[['movie_id', 'title_kor']].to_dict('records')
        

        return result


    def search_hash(self, keyword):
        subword_hash = self.search_dao.subword_hash
        name_id_hash = self.search_dao.name_id_hash

        titles = subword_hash['movie_name'].get(keyword) or []
        title_result = list(self.flatten([name_id_hash['movie_name'].get(title) for title in titles]))
        
        makers = subword_hash['maker'].get(keyword) or []
        maker_result = list(self.flatten([name_id_hash['maker'].get(maker) for maker in makers]))
        
        genres = subword_hash['genre'].get(keyword) or []
        genre_result = list(self.flatten([name_id_hash['genre'].get(genre) for genre in genres]))[:30]
        
        nations = subword_hash['nation'].get(keyword) or []
        nation_result = list(self.flatten([name_id_hash['nation'].get(nation) for nation in nations]))[:30]
        
        keyword_li = titles + makers + genres + nations
        if len(keyword_li) == 0:
            return {
                'movies': [],
                'similar_words': []
            }
        keyword_li = self.get_unique_ordered_list(keyword_li)[:15]
        
        # 영화 병합
        rec = title_result + maker_result + genre_result + nation_result
        rec = self.get_unique_ordered_list(rec)
        
        return {
            'movies': rec,
            'similar_words': keyword_li
        }


    def search_movie_by_vector(self, keyword):
        model = self.word_model.fasttext_word_model

        # get keyword vector
        tokens = self.komoran.morphs(keyword)
        vectors = np.array([model.wv[token] for token in tokens])
        keyword_vector = np.sum(vectors, axis=0)
        movie_ids = self.get_movies_by_vector(keyword_vector)

        return movie_ids


    def get_movies_by_vector(self, vector):
        cluster_df = self.word_model.cluster_df
        movie_vectors = self.word_model.movie_vectors

        # find closest cluster
        cluster_distances = cluster_df['vector'].map(lambda x: cosine(x, vector))
        target_clusters = cluster_distances.sort_values(ascending=True).index[:len(cluster_distances) // 20]

        # find closest movie in target cluster
        filtered_df = movie_vectors[movie_vectors['cluster'].isin(target_clusters)]
        distances = filtered_df['vector'].map(lambda x: cosine(x, vector))

        # test
        movie_ids = filtered_df.assign(distance=distances).sort_values(by='distance').index[:70]
        return list(movie_ids)

        '''
        movie_id = filtered_df.assign(distance=distances).sort_values(by='distance').index[0]

        # dont's return rec if the closest movie is too far from keyword
        target_vector = movie_vectors.loc[movie_id]['vector']
        if cosine(target_vector, vector) > 0.3:
            return []

        # get similar rec
        similar_rec = self.rec_dao.similar_rec.get(movie_id)

        return list(map(lambda x: x['movie_id'], similar_rec or []))
        '''
        

    def get_similar_words_by_vector(self, keyword):
        model = self.word_model.fasttext_word_model
        sim_word_tuples = model.wv.most_similar(keyword)
        return [word for word, sim in sim_word_tuples if sim > .8]


    def get_unique_ordered_list(self, li):
        seen = set()
        return [x for x in li if not (x in seen or seen.add(x))]