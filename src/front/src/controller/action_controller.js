class ActionController {
  constructor() {
    this.frontScrollLock = {
      X: null,
      Y: null
    }

    window.addEventListener('scroll', () => {
      if (this.frontScrollLock['X'] !== null && this.frontScrollLock['Y'] !== null) {
        window.scrollTo({
          top: this.frontScrollLock['Y'], 
          left: this.frontScrollLock['X'], 
          behavior: 'auto'
        });
      }
    });
  }

  lockFrontScroll = () => {
    this.frontScrollLock['X'] = window.scrollX;
    this.frontScrollLock['Y'] = window.scrollY;
  }

  unlockFrontScroll = () => {
    this.frontScrollLock['X'] = null;
    this.frontScrollLock['Y'] = null;
  }
}

export default ActionController;