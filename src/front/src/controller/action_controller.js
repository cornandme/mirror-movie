class ActionController {
  lockFrontScroll = () => {
    document.body.style.position = 'fixed';
    document.body.style.top = `-${window.scrollY}px`;
  }

  unlockFrontScroll = () => {
    document.body.style.position = '';
    document.body.style.top = '';
  }
}

export default ActionController;