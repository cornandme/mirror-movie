import React, { PureComponent } from 'react';

import styles from './nav.module.css';
import SearchResult from '../search_result/search_result';


class Nav extends PureComponent {
  handleClickTitle = () => {
    window.scroll({top: 0});
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
              onChange={this.handleKeywordInput}
            />
          </div>
        </nav>
        <SearchResult 
          stopSearch={this.props.stopSearch}
          search={this.props.search}
          getMovie={this.props.getMovie}
          searchResult={this.props.searchResult}
          lastKeyword={this.props.lastKeyword}
        />
      </>
    );
  }
}

export default Nav;