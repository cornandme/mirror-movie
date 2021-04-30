import React, { PureComponent } from 'react';
import { Route, withRouter } from 'react-router-dom';

import styles from './app.module.css';
import Front from './components/front/front';
import MovieInfo from './components/movie_info/movie_info';


class App extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      data: null,
      movieData: null,
      currentY: null,
      searching: false,
      searchResult: null,
      lastKeyword: null,
    }
  }

  initState = async () => {
    const data = await this.props.movieService.getFrontData();
    this.setState({ data });
  }

  getMovie = async (movie_id) => {
    const movieData = await this.props.movieService.getMovieData(movie_id);
    this.setState({ movieData });
  }

  stopDetail = () => {
    this.setState({ movieData: null });
  }

  startSearch = () => {
    this.setState({ searching: true });
  }

  stopSearch = () => {
    this.setState({
      searching: false,
      searchResult: null,
      lastKeyword: null,
    });
  }

  search = async (keyword) => {
    if (!keyword) {
      this.setState({
        searchResult: {
          'movies': [],
          'similar_words': [],
        },
        lastKeyword: null
      });
    } else if (keyword === this.state.lastKeyword) {
      return;
    } else {
      const searchResult = await this.props.movieService.getKeywordSearchResult(keyword);
      const lastKeyword = keyword;
      this.setState({ searchResult, lastKeyword });
    }
  }

  componentDidMount() {
    console.log('update checking after mount...');
    this.initState();
  }

  render() {
    console.log('rendering <App>. props:', this.props, 'state:', this.state);
    return (
      <div className={styles.app}>
        <Route exact path="/">
          <Front 
            data={this.state.data}
            movieData={this.state.movieData}
            searching={this.state.searching}
            searchResult={this.state.searchResult}
            lastKeyword={this.state.lastKeyword}
            getMovie={this.getMovie}
            startSearch={this.startSearch}
            stopSearch={this.stopSearch}
            search={this.search}
          />
          <MovieInfo 
            movieData={this.state.movieData}
            getMovie={this.getMovie}
            stopDetail={this.stopDetail}
          />
        </Route>
      </div>
    )
  }
}

export default withRouter(App);