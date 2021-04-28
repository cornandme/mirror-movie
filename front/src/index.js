import React from "react";
import ReactDOM from "react-dom";
import { BrowserRouter, Route } from "react-router-dom";
import styles from "./index.module.css";

import MovieService from "./service/movie_service";
import App from "./app.jsx";

const movieService = new MovieService();

ReactDOM.render(
  <React.StrictMode>
    <BrowserRouter>
      <Route path="/">
        <App movieService={movieService} />
      </Route>
    </BrowserRouter>
  </React.StrictMode>,
  document.getElementById("root")
);
