import random

class RecService(object):
    def __init__(self, rec_dao):
        self.rec_dao = rec_dao

    def get_front_rec(self):
        newest_rec = self.rec_dao.newest_rec
        cluster_rec = self.rec_dao.cluster_rec
        genre_rec = self.rec_dao.genre_rec

        # transform
        newest = [{'최근개봉': newest_rec['newest_rec']}]
        cluster = [{key: cluster_rec[key]} for key in cluster_rec.keys()]
        genre = [{key: genre_rec[key]} for key in genre_rec.keys()]

        random.shuffle(cluster)
        random.shuffle(genre)

        return newest + cluster + genre
        
    def get_newest_rec(self):
        return self.rec_dao.newest_rec['newest_rec']

    def get_cluster_rec(self):
        return self.rec_dao.cluster_rec

    def get_genre_rec(self):
        return self.rec_dao.genre_rec

    def get_actor_rec(self, movie_id):
        return self.rec_dao.actor_rec.get(movie_id)

    def get_director_rec(self, movie_id):
        return self.rec_dao.director_rec.get(movie_id)

    def get_similar_rec(self, movie_id):
        return self.rec_dao.similar_rec.get(movie_id)
