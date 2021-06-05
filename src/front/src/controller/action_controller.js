class ActionController {
  constructor() {
    this.frontScrollLockX = null;
    this.frontScrollLockY = null;

    window.addEventListener('scroll', () => {
      if (this.frontScrollLockX !== null && this.frontScrollLockY !== null) {
        window.scrollTo({
          top: this.frontScrollLockY, 
          left: this.frontScrollLockX, 
          behavior: 'auto'
        });
      }
    });
  }

  lockFrontScroll = () => {
    this.frontScrollLockX = window.scrollX;
    this.frontScrollLockY = window.scrollY;
  }

  unlockFrontScroll = () => {
    this.frontScrollLockX = null;
    this.frontScrollLockY = null;
  }
}

export default ActionController;