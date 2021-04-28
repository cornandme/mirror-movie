import React, { PureComponent } from 'react';
import { Route, withRouter } from 'react-router-dom';

import styles from './app.module.css';
import Front from './components/front/front';


class App extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      data: null
    }
  }

  initState = async () => {
    const data = await this.props.movieService.getFrontData();
    this.setState({ data });
  }

  componentDidMount() {
    console.log('update checking after mount...');
    this.initState();
  }

  render() {
    console.log('rendering <App>. props:', this.props, 'state:', this.state);
    return (
      <div className={styles.app}>
        <Route exact path="/">
          <Front data={this.state.data}/>
        </Route>
      </div>
    )
  }
}

export default withRouter(App);