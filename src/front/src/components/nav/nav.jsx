import React, { PureComponent } from 'react';

import styles from './nav.module.css';
import SearchResult from '../search_result/search_result';


class Nav extends PureComponent {
  constructor(props) {
    super(props);
    this.throttled = this.throttle(this.handleKeywordInput, 250);
  }

  handleClickTitle = () => {
    window.scroll({ top: 0 });
  }

  handleClickSearch = () => {
    this.props.startSearch();
  }

  handleClickFallbackArrow = () => {
    this.props.stopSearch();
  }

  handleKeywordInput = (event) => {
    const keyword = event.target.value;
    this.props.search(keyword);
  }

  throttle = (callback, interval) => {
    let lastCallback;
    let lastCall = Date.now() - (interval + 1);

    return (...args) => {
      clearTimeout(lastCallback);
      lastCallback = setTimeout(() => {
        callback.apply(this, args);
        lastCall = Date.now();
      }, interval - (Date.now() - lastCall));
    }
  }

  render() {
    if (!this.props.searching) {
      return (
        <nav className={styles.navBar}>
          <img
            className={styles.titleImage}
            src="/images/title.png"
            alt="title"
            onClick={this.handleClickTitle}
          />
          <div className={styles.searchIcon} onClick={this.handleClickSearch}>
            <i class="fas fa-search fa-xs"></i>
          </div>
        </nav>
      );
    }
    return (
      <>
        <nav className={styles.navBar}>
          <div className={styles.fallBackArrowIcon} onClick={this.handleClickFallbackArrow}>
            <i class="fas fa-arrow-left fa-xs"></i>
          </div>
          <div className={styles.searchBox}>
            <div className={styles.searchIcon}>
              <i class="fas fa-search fa-xs"></i>
            </div>
            <input
              className={styles.searchInput}
              type="text"
              name="search input"
              autocomplete="off"
              placeholder="검색"
              autoFocus
              required
              maxlength="20"
              onChange={this.throttled}
            />
          </div>
        </nav>
        <SearchResult
          stopSearch={this.props.stopSearch}
          search={this.props.search}
          getMovie={this.props.getMovie}
          searchResult={this.props.searchResult}
          lastKeyword={this.props.lastKeyword}
          stillcutWidth={this.props.searchStillcutWidth}
          stillcutHeight={this.props.searchStillcutHeight}
        />
      </>
    );
  }
}

export default Nav;