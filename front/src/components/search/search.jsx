import React, { Component } from 'react';

import styles from './search.module.css';


class Search extends Component {
  render() {
    console.log('rendering <Search>', this.props, this.state);
    return (
      <span>Search</span>
    );
  }
}

export default Search;