import React, { Component } from 'react';

import styles from './search_result.module.css';


class SearchResult extends Component {
  handleClickModalBlock = () => {
    this.props.stopSearch();
  }

  handleClickSimilarWord = (event) => {
    const keyword = event.currentTarget.textContent;
    this.props.search(keyword);
  }

  handleClickMovie = (event) => {
    const movie_id = event.target.getAttribute('alt');
    this.props.getMovie(movie_id);
  }

  render() {
    const searchResultResizer = this.props.resizer.searchResult;
    if (!this.props.searchResult | !this.props.lastKeyword) {
      return (
        <div
          className={styles.modalBlock}
          style={{ flexDirection: searchResultResizer.modalBlockFlexDirection }}
          onClick={this.handleClickModalBlock}
        ></div>
      );
    } else if (!this.props.searchResult.movies) {
      return (
        <div
          className={`${styles.modalBlock} ${styles.result}`}
          style={{ flexDirection: searchResultResizer.modalBlockFlexDirection }}
        >
          <div className={styles.noResultMessage}>
            <span>{`입력하신 검색어(${this.props.lastKeyword})와 일치하는 결과가 없습니다.`}</span>
          </div>
        </div>
      )
    }
    return (
      <div
        className={`${styles.modalBlock} ${styles.result}`}
        style={{ flexDirection: searchResultResizer.modalBlockFlexDirection }}
      >
        <div className={styles.stillcutBlock}>
          <div className={styles.stillcutTitle}>{`<${this.props.lastKeyword}> 관련 영화`}</div>
          <ul className={styles.stillcuts}>
            {this.props.searchResult.movies && this.props.searchResult.movies.map((movie) => {
              const src = `${process.env.REACT_APP_STILLCUT_SOURCE}${movie.movie_id}.jpg`;
              return (
                <li
                  className={styles.stillcutContainer}
                  style={{
                    width: this.props.stillcutWidth,
                    height: this.props.stillcutHeight,
                  }}
                >
                  <img
                    className={styles.stillcut}
                    src={`${src}`}
                    alt={`${movie.movie_id}`}
                    onClick={this.handleClickMovie}
                  />
                  <span
                    className={styles.stillcutMovieTitle}
                    style={{ bottom: searchResultResizer.stillcutMovieTitleBottom }}
                  >{movie.title_kor}</span>
                </li>
              );
            })}
          </ul>
        </div>
        <div
          className={styles.similarWordBlock}
          style={{ marginTop: searchResultResizer.similarWordBlockMarginTop }}
        >
          <div className={styles.similarWordTitle}>추천 검색어</div>
          <ul className={styles.similarWords}>
            {this.props.searchResult.similar_words && this.props.searchResult.similar_words.map((word) => {
              return (
                <li>
                  <div className={styles.similarWord} onClick={this.handleClickSimilarWord}>{word}</div>
                </li>
              )
            })}
          </ul>
        </div>
      </div>
    );
  }
}

export default SearchResult;