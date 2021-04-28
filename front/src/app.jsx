import React, { PureComponent } from 'react';
import { Route, withRouter } from 'react-router-dom';

import styles from './app.module.css';
import Front from './components/front/front';


class App extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      test: null,
    }
  }

  render() {
    console.log('rendering <App>', this.props, this.state);
    return (
      <div className={styles.app}>
        <Route exact path="/">
          <Front/>
        </Route>
      </div>
    )
  }
}

export default withRouter(App);