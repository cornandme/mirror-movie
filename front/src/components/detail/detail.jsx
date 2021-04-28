import React, { Component } from 'react';
import { Route, withRouter } from 'react-router-dom';

import styles from './detail.module.css';
import Header from '../header/header';


class Detail extends Component {
  render() {
    console.log('rendering <Detail>', this.props, this.state);
    return (
      <>
        <Header />
        <span>Detail</span>
      </>
    );
  }
}

export default withRouter(Detail);