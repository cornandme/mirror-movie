import React, { PureComponent } from "react";
import { Route, withRouter } from "react-router-dom";
import styles from "./app.module.css";

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
          <h1>front page</h1>
        </Route>
      </div>
    )
  }
}

export default withRouter(App);