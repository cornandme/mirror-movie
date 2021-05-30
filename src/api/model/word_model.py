from .s3_loader import S3Loader

class WordModel(S3Loader):
    def __init__(self, s3, config):
        super().__init__(s3, config)
        self.load_all_models()

    @property
    def fasttext_word_model(self):
        return self.__fasttext_word_model

    @fasttext_word_model.setter
    def fasttext_word_model(self, new_model):
        self.__fasttext_word_model = new_model

    @property
    def cluster_df(self):
        return self.__cluster_df

    @cluster_df.setter
    def cluster_df(self, new_data):
        self.__cluster_df = new_data
    
    @property
    def movie_vectors(self):
        return self.__movie_vectors

    @movie_vectors.setter
    def movie_vectors(self, new_data):
        self.__movie_vectors = new_data

    def load_all_models(self):
        self.fasttext_word_model = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['MODEL']['MODEL_PATH'])
        self.cluster_df = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['MODEL']['CLUSTER_PATH'])
        self.movie_vectors = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['MODEL']['MOVIE_VECTORS_PATH'])