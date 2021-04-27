class RecService(object):
    def __init__(self, rec_dao):
        self.rec_dao = rec_dao
        
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
