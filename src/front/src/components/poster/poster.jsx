import React, { Component } from 'react';

import styles from './poster.module.css';

class Poster extends Component {
  handleMovieClick = (event) => {
    const movie_id = event.target.getAttribute('alt');
    this.props.getMovie(movie_id);
  }
  
  render() {
    return (
      <img 
        className={styles.poster} 
        src={`${this.props.src}`} 
        alt={`${this.props.id}`}
        onClick={this.handleMovieClick}
      />
    );
  }
}

export default Poster;