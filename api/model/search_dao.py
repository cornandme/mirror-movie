from .s3_loader import S3Loader

class SearchDAO(S3Loader):
    def __init__(self, s3, config):
        super().__init__(s3, config)
        self.load_data()
        
    @property
    def subword_hash(self):
        return self.__subword_hash

    @subword_hash.setter
    def subword_hash(self, new_data):
        self.__subword_hash = new_data
    
    @property
    def name_id_hash(self):
        return self.__name_id_hash

    @name_id_hash.setter
    def name_id_hash(self, new_data):
        self.__name_id_hash = new_data
    
    def load_data(self):
        self.subword_hash = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['DATA']['SUBWORD_HASH'])
        self.name_id_hash = self.load_from_s3(self.config['AWS']['S3_BUCKET'], self.config['DATA']['NAME_ID_HASH'])