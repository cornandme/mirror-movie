import React from "react";
import ReactDOM from "react-dom";
import { BrowserRouter, Route } from "react-router-dom";
import styles from "./index.module.css";
import App from "./app.jsx";

ReactDOM.render(
  <React.StrictMode>
    <BrowserRouter>
      <Route path="/">
        <App />
      </Route>
    </BrowserRouter>
  </React.StrictMode>,
  document.getElementById("root")
);
