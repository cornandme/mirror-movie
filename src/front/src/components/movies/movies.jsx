import React, { PureComponent } from 'react';

import styles from './movies.module.css';
import Poster from '../poster/poster';


class Movies extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      pointer: 0,
      pointerMax: this.props.movies.length - this.props.posterCount,
      scrollButtonWidth: 0.25 * this.props.posterWidth,
      oneLoop: this.props.posterCount * (this.props.posterWidth + 10),
      leftScrollButtonDisplay: false,
      rightScrollButtonDisplay: false
    }
    this.movieListRef = React.createRef();
    this.leftScrollButtonRef = React.createRef();
    this.rightScrollButtonRef = React.createRef();
  }

  componentDidUpdate() {
    const pointerMax = this.props.movies.length - this.props.posterCount;
    this.setState({ pointerMax });
  }

  onMovieListHover = () => {
    if (this.props.resizer.isMobileOrTablet) {
      return;
    }
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
    if (this.props.resizer.isMobileOrTablet) {
      return;
    }
    this.setState({
      leftScrollButtonDisplay: false,
      rightScrollButtonDisplay: false
    });
  }

  onLeftScrollButtonClick = () => {
    const pointer = Math.max(0, this.state.pointer - this.props.posterCount);
    this.moveMovieList(pointer);
    this.setState({ pointer }, this.onMovieListHover);
  }

  onRightScrollButtonClick = () => {
    const pointer = Math.min(this.state.pointerMax, this.state.pointer + this.props.posterCount);
    this.moveMovieList(pointer);
    this.setState({ pointer }, this.onMovieListHover);
  }

  moveMovieList = (pointer) => {
    const length = pointer * (this.props.posterWidth + 10);
    this.movieListRef.current.style.transform = `translateX(${-length}px)`;
  }

  render() {
    const moviesResizer = this.props.resizer;
    return (
      <section
        className={styles.topicArea}
        style={{ padding: moviesResizer.movies.topicAreaPadding }}
      >
        <h4
          className={styles.topicTitle}
          style={{ fontSize: moviesResizer.movies.topicTitleFontSize }}
        >
          {this.props.id && this.props.id}
        </h4>
        <ul
          ref={this.movieListRef}
          className={styles.movieList}
          style={{ width: this.props.movies.length * (this.props.posterWidth + 10) }}
          onMouseOver={this.onMovieListHover}
          onMouseLeave={this.onMovieListLeave}
        >
          {this.props.movies &&
            this.props.movies.map((movie) => {
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
              ref={this.leftScrollButtonRef}
              className={`${styles.scrollButtons} ${styles.leftScrollButton}`}
              style={{
                width: this.state.scrollButtonWidth,
                left: this.state.pointer * (this.props.posterWidth + 10)
              }}
              onClick={this.onLeftScrollButtonClick}
            >
              <i class="fas fa-chevron-left"></i>
            </div>
          }
          {this.state.rightScrollButtonDisplay &&
            <div
              ref={this.rightScrollButtonRef}
              className={`${styles.scrollButtons} ${styles.rightScrollButton}`}
              style={{
                width: this.state.scrollButtonWidth,
                left: (this.state.oneLoop - this.state.scrollButtonWidth) + this.state.pointer * (this.props.posterWidth + 10)
              }}
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