from .s3_loader import S3Loader

class RecDAO(S3Loader):
    def __init__(self, s3, config):
        super().__init__(s3, config)
        self.load_all_recs()

    @property
    def newest_rec(self):
        return self.__newest_rec

    @newest_rec.setter
    def newest_rec(self, new_data):
        self.__newest_rec = new_data

    @property
    def cluster_rec(self):
        return self.__cluster_rec

    @cluster_rec.setter
    def cluster_rec(self, new_data):
        self.__cluster_rec = new_data

    @property
    def genre_rec(self):
        return self.__genre_rec

    @genre_rec.setter
    def genre_rec(self, new_data):
        self.__genre_rec = new_data

    @property
    def actor_rec(self):
        return self.__actor_rec

    @actor_rec.setter
    def actor_rec(self, new_data):
        self.__actor_rec = new_data

    @property
    def director_rec(self):
        return self.__director_rec

    @director_rec.setter
    def director_rec(self, new_data):
        self.__director_rec = new_data

    @property
    def similar_rec(self):
        return self.__similar_rec

    @similar_rec.setter
    def similar_rec(self, new_data):
        self.__similar_rec = new_data

    def load_all_recs(self):
        self.newest_rec = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['REC']['FRONT_NEWEST'])
        self.cluster_rec = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['REC']['FRONT_CLUSTER'])
        self.genre_rec = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['REC']['FRONT_GENRE'])
        self.actor_rec = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['REC']['DETAIL_ACTOR'])
        self.director_rec = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['REC']['DETAIL_DIRECTOR'])
        self.similar_rec = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['REC']['DETAIL_SIMILAR'])
        print(len(self.newest_rec), len(self.cluster_rec), len(self.genre_rec), len(self.actor_rec), len(self.director_rec), len(self.similar_rec))