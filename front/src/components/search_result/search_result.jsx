import React, { Component } from 'react';

import styles from './search_result.module.css';


class SearchResult extends Component {
  handleClickModal = () => {
    this.props.stopSearch();
  }

  render() {
    console.log('rendering <SearchResult>', this.props, this.state);
    if (!this.props.searchResult) {
      return (
        <div className={styles.modalBlock} onClick={this.handleClickModal}></div>
      );
    }
    return (
      <div>
        <ul>
          {this.props.searchResult.map((movie) => {
            const src = `${process.env.REACT_APP_STILLCUT_SOURCE}${movie.movie_id}.jpg`;
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
      </div>
    );
  }
}

export default SearchResult;