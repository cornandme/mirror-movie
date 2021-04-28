import React, { PureComponent } from 'react';

import styles from './header.module.css';
import Search from '../search/search';


class Header extends PureComponent {
  render() {
    console.log('rendering <Header>', this.props, this.state);
    return (
      <>
        <span>header</span>
        <Search />
      </>
    );
  }
}

export default Header;