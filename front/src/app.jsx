import React, { PureComponent } from 'react';
import { Route, withRouter } from 'react-router-dom';

import styles from './app.module.css';
import Front from './components/front/front';
import MovieInfo from './components/movie_info/movie_info';


class App extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      dimensionX: window.innerWidth,
      frontData: null,
      frontPosterCount: null,
      frontPosterWidth: null,
      frontPosterHeight: null,
      movieData: null,
      searching: false,
      searchResult: null,
      lastKeyword: null,
    }
  }

  getfrontData = async () => {
    const frontData = await this.props.movieService.getFrontData();
    this.setState({ frontData });
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

  handleResize = () => {
    const dimensionX = document.body.scrollWidth;
    const frontPosterCount = this.props.resizer.getFrontPosterCount(dimensionX);
    const [frontPosterWidth, frontPosterHeight] = this.props.resizer.getFrontPosterSize(dimensionX, frontPosterCount);
    this.setState({ dimensionX, frontPosterCount, frontPosterWidth, frontPosterHeight });
  }

  componentDidMount() {
    console.log('update checking after mount...');
    this.getfrontData();
    this.handleResize();
    window.addEventListener('resize', this.handleResize);
  }

  render() {
    return (
      <div className={styles.app}>
        <Route exact path="/">
          <Front 
            frontData={this.state.frontData}
            frontPosterCount={this.state.frontPosterCount}
            frontPosterWidth={this.state.frontPosterWidth}
            frontPosterHeight={this.state.frontPosterHeight}
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