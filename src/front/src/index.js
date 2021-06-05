import React from "react";
import ReactDOM from "react-dom";
import { BrowserRouter, Route } from "react-router-dom";
import styles from "./index.module.css";

import MovieService from "./service/movie_service";
import Resizer from "./controller/resizer";
import ActionController from "./controller/action_controller";

import App from "./app.jsx";

const movieService = new MovieService();
const resizer = new Resizer();
const actionController = new ActionController();

ReactDOM.render(
  <React.StrictMode>
    <BrowserRouter>
      <Route path="/">
        <App 
          movieService={movieService} 
          resizer={resizer} 
          actionController={actionController}
        />
      </Route>
    </BrowserRouter>
  </React.StrictMode>,
  document.getElementById("root")
);
