import React, { PureComponent } from 'react';

import styles from './nav.module.css';
import Search from '../search/search';


class Nav extends PureComponent {
  render() {
    console.log('rendering <Nav>', this.props, this.state);
    return (
      <>
        <span>nav</span>
        <Search />
      </>
    );
  }
}

export default Nav;