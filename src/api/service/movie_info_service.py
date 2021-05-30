import pandas as pd

class MovieInfoService(object):
    def __init__(self, movie_info_dao):
        self.movie_info_dao = movie_info_dao

    def get_movie_info(self, movie_id):
        movie_info = self.movie_info_dao.movie_info
        movie_info = movie_info[movie_info['movie_id'] == movie_id]
        movie_info = movie_info.where(pd.notnull(movie_info), None)
        return movie_info.to_dict('records')