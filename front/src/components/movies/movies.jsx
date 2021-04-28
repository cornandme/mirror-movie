import React, { PureComponent } from 'react';

import styles from './movies.module.css'

class Movies extends PureComponent {
  render() {
    console.log('rendering <Movies>. props:', this.props, 'state:', this.state);
    return (
      <div>
        <h4>{this.props.id && this.props.id}</h4>
        <ul className={styles.movieList}>
          {this.props.movies && this.props.movies.map((movie) => (
            <li><span>{movie.movie_id}</span></li>
          ))}
        </ul>
      </div>
    );
  }
}

export default Movies;