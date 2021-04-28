class MovieService {
  getFrontData = async () => {
    const url = process.env.REACT_APP_API_HOME;
    console.log(url);
    const res = await fetch(url, {
      method: "GET",
    });
    return await res.json();
  };
}

export default MovieService;
