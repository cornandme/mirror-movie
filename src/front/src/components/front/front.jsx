import React, { Component } from 'react';

import styles from './front.module.css';
import Nav from '../nav/nav';
import Movies from '../movies/movies';


class Front extends Component {
  constructor(props) {
    super(props);
    this.state = {
      bannerImgNum: null,
    }
  }

  componentDidMount() {
    this.pickBannerImage();
    this.bannerInterval = setInterval(() => this.pickBannerImage, 1800000);
  }

  componentWillUnmount() {
    clearInterval(this.bannerInterval);
  }

  pickBannerImage = () => {
    const date = new Date();
    const bannerImgNum = (date.getDate() + date.getHours()) % 11;
    // const bannerImgNum = Math.floor(Math.random() * 11)
    this.setState({ bannerImgNum });
  }

  render() {
    const bannerImgPath = `${process.env.REACT_APP_BANNER_IMG_SOURCE}banner_${this.state.bannerImgNum}.jpg`;
    return (
      <>
        <Nav
          startSearch={this.props.startSearch}
          stopSearch={this.props.stopSearch}
          search={this.props.search}
          getMovie={this.props.getMovie}
          searching={this.props.searching}
          searchResult={this.props.searchResult}
          lastKeyword={this.props.lastKeyword}
          searchStillcutWidth={this.props.searchStillcutWidth}
          searchStillcutHeight={this.props.searchStillcutHeight}
          resizer={this.props.resizer}
        />
        <main
          className={styles.main}
        >
          <header
            className={styles.headerBox}
            style={{
              'height': this.props.bannerImageHeight
            }}
          >
            <img
              className={styles.bannerImage}
              src={bannerImgPath}
              alt="banner"
            />
          </header>
          <div className={styles.movieListBoardBg}>
            <ul
              className={styles.movieListBoard}
              style={{
                top: `${-this.props.movieListBoardMove}px`,
                paddingTop: `${this.props.movieListBoardMove / 2}px`,
                background: `linear-gradient(to bottom, rgba(0, 0, 0, 0) 0, rgba(0, 0, 0, 1) ${this.props.movieListBoardMove}px)`
              }}
            >
              {this.props.frontData &&
                this.props.frontData.front_rec.map((obj) => {
                  return (
                    <Movies
                      key={Object.keys(obj)[0]}
                      id={Object.keys(obj)[0]}
                      movies={Object.values(obj)[0]}
                      getMovie={this.props.getMovie}
                      posterCount={this.props.frontPosterCount}
                      posterWidth={this.props.frontPosterWidth}
                      posterHeight={this.props.frontPosterHeight}
                      resizer={this.props.resizer}
                    />
                  );
                })
              }
            </ul>
          </div>
        </main>
      </>
    );
  }
}

export default Front;