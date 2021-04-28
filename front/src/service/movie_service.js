class MovieService {
  constructor() {
    this.home = process.env.REACT_APP_API_HOME;
  }

  getFrontData = async () => {
    try {
      const res = await fetch(this.home, {
        method: "GET",
        headers: { Accept: "application/json" },
      });
      return await res.json();
    } catch (e) {
      console.error(`fetch failed. error: ${e}`);
    }
  };
}

export default MovieService;
