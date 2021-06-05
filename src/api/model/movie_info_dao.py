from .s3_loader import S3Loader

class MovieInfoDAO(S3Loader):
    def __init__(self, s3, config):
        super().__init__(s3, config)
        self.load_movie_info()

    @property
    def movie_info(self):
        return self.__movie_info

    @movie_info.setter
    def movie_info(self, new_data):
        self.__movie_info = new_data
    
    def load_movie_info(self):
        self.movie_info = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['DATA']['MOVIE_INFO'])
        self.movie_info = self.movie_info.set_index('movie_id').sort_index()
        self.movie_info.drop(columns=['poster_url', 'stillcut_url'], inplace=True)
        