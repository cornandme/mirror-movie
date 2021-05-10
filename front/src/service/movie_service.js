class MovieService {
  constructor() {
    this.origin = process.env.REACT_APP_REQUEST_ORIGIN;
    this.home = process.env.REACT_APP_API_HOME;
  }

  getFrontData = async () => {
    try {
      const res = await fetch(this.home, {
        method: "GET",
        headers: {
          Origin: this.origin,
          Accept: "application/json",
        },
      });
      return await res.json();
    } catch (e) {
      console.error(`fetch failed. error: ${e}`);
    }
  };

  getMovieData = async (movie_id) => {
    const url = `${this.home}movie/${movie_id}`;
    try {
      const res = await fetch(url, {
        method: "GET",
        headers: {
          Origin: this.origin,
          Accept: "application/json",
        },
      });
      return await res.json();
    } catch (e) {
      console.error(`fetch failed. error: ${e}`);
    }
  };

  getKeywordSearchResult = async (keyword) => {
    const url = `${this.home}search/${keyword}`;
    try {
      const res = await fetch(url, {
        method: "GET",
        headers: {
          Origin: this.origin,
          Accept: "application/json",
        },
      });
      return await res.json();
    } catch (e) {
      console.error(`fetch failed. error: ${e}`);
    }
  };
}

export default MovieService;
