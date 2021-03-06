import React, { Component } from 'react';

import styles from './movie_info.module.css';
import MoviesInDetail from '../movies_in_detail/movies_in_detail';

class MovieInfo extends Component {
  constructor(props) {
    super(props);
    this.infoBoxRef = React.createRef();
  }

  handleClickModalBlock = () => {
    this.props.stopDetail();
  }

  handleClickBackbutton = () => {
    this.props.stopDetail();
  }

  componentDidMount() {
    if (this.props.movieData) {
      document.body.style.overflow = 'hidden';
    }
  }

  componentWillUnmount() {
    document.body.style.overflow = 'scroll';
  }

  componentDidUpdate() {
    if (this.props.movieData) {
      this.infoBoxRef.current.scrollTop = 0;
    }
  }

  render() {
    return (
      <>
        <div className={styles.modalBlock} onClick={this.handleClickModalBlock}></div>
        <div
          ref={this.infoBoxRef}
          className={styles.infoBox}
          style={{
            'width': this.props.detailInfoboxWidth
          }}
        >
          <div
            className={styles.stillcutContainer}
            style={{
              'height': this.props.detailStillcutHeight
            }}
          >
            <img
              className={styles.stillcut}
              src={`${process.env.REACT_APP_STILLCUT_SOURCE}${this.props.movieData.movie_info.movie_id}.jpg`}
              alt={`${this.props.movieData.movie_info.movie_id}`}
            />
            <h3 className={styles.movieTitle}>{this.props.movieData.movie_info.title_kor}</h3>
            {!this.props.resizer.isMobileOnly &&
              <div
                className={styles.xbutton}
                onClick={this.handleClickBackbutton}
              >
                <i class="fas fa-times"></i>
              </div>
            }
            {this.props.resizer.isMobileOnly &&
              <div
                className={styles.arrowbutton}
                onClick={this.handleClickBackbutton}
              >
                <i class="fas fa-chevron-left fa-3x"></i>
              </div>
            }
          </div>
          <div className={styles.infoSection}>
            <span className={styles.info}>
              {this.props.movieData.movie_info.release_year && this.props.movieData.movie_info.release_year}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info.running_time && this.props.movieData.movie_info.running_time}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info.story && this.props.movieData.movie_info.story}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info.main_actors &&
                `??????: ${this.props.movieData.movie_info.main_actors.join(", ")}`}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info.director &&
                `??????: ${this.props.movieData.movie_info.director}`}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info.writer &&
                `??????: ${this.props.movieData.movie_info.writer}`}
            </span>
          </div>
          <div className={styles.recSection}>
            {this.props.movieData.similar_rec && this.props.movieData.similar_rec.length > 0 &&
              <div className={styles.recContainer}>
                <h4 className={styles.recTitle}>????????? ??????</h4>
                <MoviesInDetail
                  key='similar_rec'
                  movies={this.props.movieData.similar_rec.slice(0, 15)}
                  getMovie={this.props.getMovie}
                  detailPosterWidth={this.props.detailPosterWidth}
                  detailPosterHeight={this.props.detailPosterHeight}
                />
              </div>
            }
            {this.props.movieData.actor_rec && this.props.movieData.actor_rec.length > 0 &&
              <div className={styles.recContainer}>
                <h4 className={styles.recTitle}>????????? ????????? ??????</h4>
                <MoviesInDetail
                  key='actor_rec'
                  movies={this.props.movieData.actor_rec}
                  getMovie={this.props.getMovie}
                  detailPosterWidth={this.props.detailPosterWidth}
                  detailPosterHeight={this.props.detailPosterHeight}
                />
              </div>
            }
            {this.props.movieData.director_rec && this.props.movieData.director_rec.length > 0 &&
              <div className={styles.recContainer}>
                <h4 className={styles.recTitle}>?????????, ????????? ????????? ????????????</h4>
                <MoviesInDetail
                  key='director_rec'
                  movies={this.props.movieData.director_rec}
                  getMovie={this.props.getMovie}
                  detailPosterWidth={this.props.detailPosterWidth}
                  detailPosterHeight={this.props.detailPosterHeight}
                />
              </div>
            }
          </div>
        </div>
      </>
    );
  }
}

export default MovieInfo;