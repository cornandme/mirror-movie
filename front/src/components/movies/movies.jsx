import React, { PureComponent } from 'react';

import styles from './movies.module.css'


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
            try {
              return (
                <li>
                  <div className={styles.posterContainer}>
                    <img className={styles.poster} src={`${src}`} alt={`${movie.movie_id}`}/>
                  </div>
                </li>
              );
            } catch (e) {
              console.error(e);
            }
          })}
        </ul>
      </section>
    );
  }
}

export default Movies;