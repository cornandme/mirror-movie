import React, { Component } from 'react';

import styles from './front.module.css';
import Nav from '../nav/nav';
import Movies from '../movies/movies';


class Front extends Component {
  render() {
    console.log('rendering <Front>. props:', this.props, 'state:', this.state);
    return (
      <>
        <Nav 
          startSearch={this.props.startSearch}
          stopSearch={this.props.stopSearch}
          search={this.props.search}
          searching={this.props.searching}
          searchResult={this.props.searchResult}
        />
        <main className={styles.main}>
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
        </main>
      </>
    );
  }
}

export default Front;