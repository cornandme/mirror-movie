import React, { Component } from 'react';

import styles from './poster.module.css';
import IntersectionObserverController from '../../controller/intersection_observer_controller';

const ioc = new IntersectionObserverController();

class Poster extends Component {
  constructor(props) {
    super(props);
    this.state = {
      isVisible: false
    }
    this.imageRef = React.createRef();
  }

  componentDidMount() {
    ioc.createObserver(this.imageRef.current, this.onIntersect);
  }

  onIntersect = ([{ isIntersecting }], observer) => {
    if (isIntersecting) {
      this.setState({ isVisible: true });
      observer.unobserve(this.imageRef.current);
    }
  }

  handleMovieClick = (event) => {
    const movie_id = event.target.getAttribute('alt');
    this.props.getMovie(movie_id);
  }

  render() {
    return (
      <div
        ref={this.imageRef}
        className={styles.observerBox}
      >
        {this.state.isVisible &&
          <img
            className={styles.poster}
            src={`${this.props.src}`}
            alt={`${this.props.id}`}
            onClick={this.handleMovieClick}
          />
        }
      </div>
    );
  }
}

export default Poster;