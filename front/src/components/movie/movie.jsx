import React, { Component } from 'react';
import { Route, withRouter } from 'react-router-dom';

import styles from './movie.module.css';
import Header from '../header/header';


class Movie extends Component {
  render() {
    console.log('rendering <Movie>', this.props, this.state);
    return (
      <>
        <Header />
        <span>Movie</span>
      </>
    );
  }
}

export default withRouter(Movie);