import React, { Component } from 'react';

import styles from './front.module.css';
import Header from '../header/header';


class Front extends Component {
  render() {
    console.log('rendering <Front>', this.props, this.state);
    return (
      <>
        <Header />
        <span>front page</span>
      </>
    );
  }
}

export default Front;