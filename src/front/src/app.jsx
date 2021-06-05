import React, { PureComponent } from 'react';
import { Route, withRouter } from 'react-router-dom';

import styles from './app.module.css';
import Front from './components/front/front';
import MovieInfo from './components/movie_info/movie_info';


class App extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      dimensionX: null,
      dimensionY: null,
      bannerImageHeight: null,
      movieListBoardMove: null,
      frontData: null,
      frontPosterCount: null,
      frontPosterWidth: null,
      frontPosterHeight: null,
      movieData: null,
      searchStillcutWidth: null,
      searchStillcutHeight: null,
      searching: false,
      searchResult: null,
      lastKeyword: null,
      detailInfoboxWidth: null,
      detailStillcutHeight: null,
      detailPosterWidth: null,
      detailPosterHeight: null
    }
  }

  componentDidMount() {
    this.getfrontData();
    this.handleResize();
    window.addEventListener('resize', this.handleResize);
  }

  handleResize = () => {
    const dimensionX = document.body.scrollWidth;
    const dimensionY = window.innerHeight;
    const resizer = this.props.resizer;
    resizer.mobileResize();
    resizer.dimensionX = dimensionX;
    resizer.dimensionY = dimensionY;

    const bannerImageHeight = this.props.resizer.getBannerImageHeight();
    const movieListBoardMove = this.props.resizer.getMovieListBoardMove();
    const frontPosterCount = this.props.resizer.getFrontPosterCount();
    const [frontPosterWidth, frontPosterHeight] = this.props.resizer.getFrontPosterSize();
    const [searchStillcutWidth, searchStillcutHeight] = this.props.resizer.getSearchStillcutSize();
    const [detailInfoboxWidth, detailStillcutHeight, detailPosterWidth, detailPosterHeight] = this.props.resizer.getDetailInfoboxSize();
    this.setState({
      dimensionX,
      dimensionY,
      bannerImageHeight,
      movieListBoardMove,
      frontPosterCount,
      frontPosterWidth,
      frontPosterHeight,
      searchStillcutWidth,
      searchStillcutHeight,
      detailInfoboxWidth,
      detailStillcutHeight,
      detailPosterWidth,
      detailPosterHeight
    });
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

  }

  }

  render() {
    return (
      <div
        className={styles.app}
        style={{ minWidth: this.props.resizer.minWidth }}
      >
        <Route exact path="/">
          <Front
            dimensionX={this.state.dimensionX}
            dimensionY={this.state.dimensionY}
            bannerImageHeight={this.state.bannerImageHeight}
            movieListBoardMove={this.state.movieListBoardMove}
            frontData={this.state.frontData}
            frontPosterCount={this.state.frontPosterCount}
            frontPosterWidth={this.state.frontPosterWidth}
            frontPosterHeight={this.state.frontPosterHeight}
            movieData={this.state.movieData}
            searchStillcutWidth={this.state.searchStillcutWidth}
            searchStillcutHeight={this.state.searchStillcutHeight}
            searching={this.state.searching}
            searchResult={this.state.searchResult}
            lastKeyword={this.state.lastKeyword}
            getMovie={this.getMovie}
            startSearch={this.startSearch}
            stopSearch={this.stopSearch}
            search={this.search}
            resizer={this.props.resizer}
          />
          {this.state.movieData &&
            <MovieInfo
              movieData={this.state.movieData}
              detailInfoboxWidth={this.state.detailInfoboxWidth}
              detailStillcutHeight={this.state.detailStillcutHeight}
              detailPosterWidth={this.state.detailPosterWidth}
              detailPosterHeight={this.state.detailPosterHeight}
              dimensionX={this.state.dimensionX}
              getMovie={this.getMovie}
              stopDetail={this.stopDetail}
              resizer={this.props.resizer}
            />
          }
        </Route>
      </div>
    )
  }
}

export default withRouter(App);