import React, { PureComponent } from 'react';

import styles from './movies.module.css';
import Poster from '../poster/poster';


class Movies extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      leftScrollButtonDisplay: false,
      rightScrollButtonDisplay: false,
      pointer: 0,
      pointerMax: this.props.movies.length - this.props.posterCount,
      scrollButtonWidth: 0.25 * this.props.posterWidth
    }
  }

  onMovieListHover = () => {
    this.determineLeftScrollButtonDisplay();
    this.determineRightScrollButtonDisplay();
  }

  determineLeftScrollButtonDisplay = () => {
    if (this.state.pointer === 0) {
      this.setState({ leftScrollButtonDisplay: false });
      return;
    }
    this.setState({ leftScrollButtonDisplay: true });
  }

  determineRightScrollButtonDisplay = () => {
    if (this.state.pointer >= this.state.pointerMax) {
      this.setState({ rightScrollButtonDisplay: false });
      return;
    }
    this.setState({ rightScrollButtonDisplay: true });
  }

  onMovieListLeave = () => {
    this.setState({
      leftScrollButtonDisplay: false,
      rightScrollButtonDisplay: false
    });
  }

  onLeftScrollButtonClick = () => {
    const pointer = Math.max(0, this.state.pointer - this.props.posterCount);
    this.setState({ pointer }, this.onMovieListHover);
  }

  onRightScrollButtonClick = () => {
    const pointer = Math.min(this.state.pointerMax, this.state.pointer + this.props.posterCount);
    this.setState({ pointer }, this.onMovieListHover);
  }

  componentDidUpdate() {
    const pointerMax = this.props.movies.length - this.props.posterCount;
    this.setState({ pointerMax });
  }

  render() {
    return (
      <section
        className={styles.topicArea}
        style={{ padding: this.props.resizer.movies.topicAreaPadding }}
      >
        <h4
          className={styles.topicTitle}
          style={{ fontSize: this.props.resizer.movies.topicTitleFontSize }}
        >
          {this.props.id && this.props.id}
        </h4>
        <ul
          className={styles.movieList}
          onMouseOver={this.onMovieListHover}
          onMouseLeave={this.onMovieListLeave}
        >
          {this.props.movies &&
            this.props.movies.slice(this.state.pointer, this.state.pointer + this.props.posterCount).map((movie) => {
              const src = `${process.env.REACT_APP_POSTER_SOURCE}${movie.movie_id}.jpg`;
              return (
                <div
                  className={styles.posterContainer}
                  style={{
                    width: this.props.posterWidth,
                    height: this.props.posterHeight
                  }}
                >
                  <Poster
                    key={movie.movie_id}
                    id={movie.movie_id}
                    src={src}
                    getMovie={this.props.getMovie}
                  />
                </div>
              );
            })}
          {this.state.leftScrollButtonDisplay &&
            <div
              className={`${styles.scrollButtons} ${styles.leftScrollButton}`}
              style={{ width: this.state.scrollButtonWidth }}
              onClick={this.onLeftScrollButtonClick}
            >
              <i class="fas fa-chevron-left"></i>
            </div>
          }
          {this.state.rightScrollButtonDisplay &&
            <div
              className={`${styles.scrollButtons} ${styles.rightScrollButton}`}
              style={{ width: this.state.scrollButtonWidth }}
              onClick={this.onRightScrollButtonClick}
            >
              <i class="fas fa-chevron-right"></i>
            </div>
          }
        </ul>
      </section>
    );
  }
}

export default Movies;