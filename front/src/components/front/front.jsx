import React, { Component } from 'react';

import styles from './front.module.css';
import Header from '../header/header';
import Movies from '../movies/movies';


class Front extends Component {
  render() {
    console.log('rendering <Front>. props:', this.props, 'state:', this.state);
    return (
      <>
        <Header />
        <ul className={styles.movieListBoard}>
          {this.props.data &&
            this.props.data.front_rec.map((obj) => {
              return (
                <Movies
                  key={Object.keys(obj)[0]}
                  id={Object.keys(obj)[0]}
                  movies={Object.values(obj)[0]}
                />
              );
            })
          }
        </ul>
      </>
    );
  }
}

export default Front;