import React, { Component } from 'react';

import styles from './movies_in_detail.module.css';
import Poster from '../poster/poster';


class MoviesInDetail extends Component {
  render() {
    console.log('rendering <MoviesInDetail>', this.props, this.state);
    return (
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
    );
  }
}

export default MoviesInDetail;