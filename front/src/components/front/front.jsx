import React, { Component } from 'react';

import styles from './front.module.css';
import Nav from '../nav/nav';
import Movies from '../movies/movies';


class Front extends Component {
  render() {
    return (
      <>
        <Nav 
          startSearch={this.props.startSearch}
          stopSearch={this.props.stopSearch}
          search={this.props.search}
          getMovie={this.props.getMovie}
          searching={this.props.searching}
          searchResult={this.props.searchResult}
          lastKeyword={this.props.lastKeyword}
        />
        <main className={styles.main}>
          <ul className={styles.movieListBoard}>
            {this.props.frontData &&
              this.props.frontData.front_rec.map((obj) => {
                return (
                  <Movies
                    key={Object.keys(obj)[0]}
                    id={Object.keys(obj)[0]}
                    movies={Object.values(obj)[0]}
                    getMovie={this.props.getMovie}
                    posterCount={this.props.frontPosterCount}
                    posterWidth={this.props.frontPosterWidth}
                    posterHeight={this.props.frontPosterHeight}
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