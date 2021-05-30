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

  handleClickXButton = () => {
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
        <div ref={this.infoBoxRef} className={styles.infoBox}>
          <div className={styles.stillcutContainer}>
            <img 
              className={styles.stillcut}
              src={`${process.env.REACT_APP_STILLCUT_SOURCE}${this.props.movieData.movie_info[0].movie_id}.jpg`}
              alt={`${this.props.movieData.movie_info[0].movie_id}`}
            />
            <h3 className={styles.movieTitle}>{this.props.movieData.movie_info[0].title_kor}</h3>
            <div 
              className={styles.xButton}
              onClick={this.handleClickXButton}
            >
              <i class="fas fa-times"></i>
            </div>
          </div>
          <div className={styles.infoSection}>
            <span className={styles.info}>
              {this.props.movieData.movie_info[0].release_year && this.props.movieData.movie_info[0].release_year}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info[0].running_time && this.props.movieData.movie_info[0].running_time}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info[0].story && this.props.movieData.movie_info[0].story}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info[0].main_actors && 
                `출연: ${this.props.movieData.movie_info[0].main_actors.join(", ")}`}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info[0].director && 
                `감독: ${this.props.movieData.movie_info[0].director}`}
            </span>
            <span className={styles.info}>
              {this.props.movieData.movie_info[0].writer && 
                `각본: ${this.props.movieData.movie_info[0].writer}`}
            </span>
          </div>
          <div className={styles.recSection}>
            {this.props.movieData.similar_rec && this.props.movieData.similar_rec.length > 0 &&
                <div className={styles.recContainer}>
                  <h4 className={styles.recTitle}>유사한 영화</h4>
                  <MoviesInDetail 
                    key='similar_rec'
                    movies={this.props.movieData.similar_rec.slice(0, 10)}
                    getMovie={this.props.getMovie}
                  />
                </div>
            }
            {this.props.movieData.actor_rec && this.props.movieData.actor_rec.length > 0 &&
              <div className={styles.recContainer}>
                <h4 className={styles.recTitle}>배우가 출연한 영화</h4>
                <MoviesInDetail 
                  key='actor_rec'
                  movies={this.props.movieData.actor_rec}
                  getMovie={this.props.getMovie}
                />
              </div>
            }
            {this.props.movieData.director_rec && this.props.movieData.director_rec.length > 0 &&
              <div className={styles.recContainer}>
                <h4 className={styles.recTitle}>스토리, 연출이 마음에 들었다면</h4>
                <MoviesInDetail 
                  key='director_rec'
                  movies={this.props.movieData.director_rec}
                  getMovie={this.props.getMovie}
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