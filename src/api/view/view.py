def create_endpoints(app, movie_info_service, rec_service, search_service):
    @app.route('/api/ping', methods=['GET'])
    def ping():
        return 'pong!'

    @app.route('/api/', methods=['GET'])
    def home():
        front_rec = rec_service.get_front_rec()
        return {
            'front_rec': front_rec
        }
    
    @app.route('/api/movies/<string:movie_id>', methods=['GET'])
    def movie(movie_id):
        movie_info = movie_info_service.get_movie_info(movie_id)
        actor_rec = rec_service.get_actor_rec(movie_id)
        director_rec = rec_service.get_director_rec(movie_id)
        similar_rec = rec_service.get_similar_rec(movie_id)
        return {
            'movie_info': movie_info,
            'actor_rec': actor_rec,
            'director_rec': director_rec,
            'similar_rec': similar_rec
        }

    @app.route('/api/search/<path:keyword>', methods=['GET'])
    def search(keyword):
        return search_service.search(keyword)