import React, { PureComponent } from 'react';

import styles from './movies.module.css';
import Poster from '../poster/poster';


class Movies extends PureComponent {
  render() {
    console.log('rendering <Movies>. props:', this.props, 'state:', this.state);
    return (
      <section className={styles.topicArea}>
        <h4 className={styles.topicTitle}>
          {this.props.id && this.props.id}
        </h4>
        <ul className={styles.movieList}>
          {this.props.movies && this.props.movies.map((movie) => {
            const src = `${process.env.REACT_APP_POSTER_SOURCE}${movie.movie_id}.jpg`;
            return (
              <li className={styles.posterContainer}>
                <Poster 
                  key={movie.movie_id}
                  id={movie.movie_id}
                  src={src}
                  getMovie={this.props.getMovie}
                />
              </li>
            );
          })}
        </ul>
      </section>
    );
  }
}

export default Movies;