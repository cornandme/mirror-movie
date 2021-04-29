import React, { PureComponent } from 'react';
import { Route, withRouter } from 'react-router-dom';

import styles from './app.module.css';
import Front from './components/front/front';


class App extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      data: null,
      searching: false,
      searchResult: null
    }
  }

  initState = async () => {
    const data = await this.props.movieService.getFrontData();
    this.setState({ data });
  }

  startSearch = () => {
    this.setState({ searching: true });
  }

  stopSearch = () => {
    this.setState({
      searching: false,
      searchResult: null
    });
  }

  search = async (keyword) => {
    if (!keyword) {
      return {}
    }
    const searchResult = await this.props.movieService.getKeywordSearchResult(keyword);
    this.setState({ searchResult: searchResult.search_result });
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
          <Front 
            data={this.state.data}
            startSearch={this.startSearch}
            stopSearch={this.stopSearch}
            search={this.search}
            searching={this.state.searching}
            searchResult={this.state.searchResult}
          />
        </Route>
      </div>
    )
  }
}

export default withRouter(App);