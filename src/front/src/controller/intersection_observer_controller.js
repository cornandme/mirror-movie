class IntersectionObserverController {
  createObserver = (target, onIntersect, options=Object()) => {
    let observer;
    options = {
      root: options['root'] || null,
      rootMargin: options['rootMargin'] || '300px',
      threshold: options['threshold'] || 0
    }

    observer = new IntersectionObserver(onIntersect, options);
    observer.observe(target);
  }
}

export default IntersectionObserverController;