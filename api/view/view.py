

def create_endpoints(app, rec_service):
    @app.route('/ping', methods=['GET'])
    def ping():
        return 'pong!'

    @app.route('/', methods=['GET'])
    def home():
        return 'home page'

    @app.route('/movie/<movie_id>', methods=['GET'])
    def movie(movie_id):
        return f'movie_id {movie_id}\'s detail page.'

    @app.route('/search/<keyword>')
    def search(keyword):
        return f'searching {keyword}'