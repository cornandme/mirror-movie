import { isMobileOnly } from 'react-device-detect';

class Resizer {
  constructor() {
    this.isMobileOnly = isMobileOnly;
    this._mobileResize()
  }

  _mobileResize = () => {
    if (isMobileOnly) {
      document.documentElement.style.fontSize = '40px';
    }

    this.searchResult = {
      modalBlockFlexDirection: isMobileOnly ? 'column' : 'row',
      similarWordBlockMarginTop: isMobileOnly ? '7rem' : 0
    }
  }

  set dimX(dimX) {
    this.dimensionX = dimX;
  }

  set dimY(dimY) {
    this.dimensionY = dimY;
  }

  getBannerImageHeight = () => {
    let bannerImageHeight = 0.7 * this.dimensionX;
    this.bannerImageHeight = Math.min(this.dimensionY, bannerImageHeight) - 50;
    return this.bannerImageHeight;
  };

  getMovieListBoardMove = () => {
    if (!this.bannerImageHeight) {
      this.getBannerImageHeight();
    }
    return 0.25 * this.bannerImageHeight;
  };

  getFrontPosterCount = () => {
    const dimensionX = isMobileOnly ? this.dimensionX / 7 : this.dimensionX;
    this.frontPosterCount = Math.round(Math.log(dimensionX) / Math.log(1.3) - 20);
    return this.frontPosterCount;
  };

  getFrontPosterSize = () => {
    const border = isMobileOnly ? 200 : 50;
    const posterMargin = isMobileOnly ? 40 : 10;
    const room = this.dimensionX - border - this.frontPosterCount * posterMargin;
    const frontPosterWidth = room / this.frontPosterCount;
    const frontPosterHeight = 1.5 * frontPosterWidth;
    return [frontPosterWidth, frontPosterHeight];
  };

  getSearchStillcutSize = () => {
    const dimensionX = isMobileOnly ? 1.5 * this.dimensionX : this.dimensionX;
    let searchStillcutWidth;
    if (dimensionX <= 875) {
      const room = dimensionX - 40;
      searchStillcutWidth = room / 2;
    } else if (dimensionX <= 1080) {
      const room = dimensionX - 40;
      searchStillcutWidth = room / 3;
    } else if (dimensionX <= 1548) {
      const room = dimensionX - 40 - 270;
      searchStillcutWidth = room / 3;
    } else if (dimensionX <= 1972) {
      const room = dimensionX - 40 - 270;
      searchStillcutWidth = room / 4;
    } else if (dimensionX <= 2400) {
      const room = dimensionX - 40 - 270;
      searchStillcutWidth = room / 5;
    } else {
      const room = dimensionX - 40 - 270;
      searchStillcutWidth = room / 6;
    }

    const searchStillcutHeight = 0.6 * searchStillcutWidth;
    return [searchStillcutWidth, searchStillcutHeight];
  };

  getDetailInfoboxSize = () => {
    const detailInfoboxWidth = isMobileOnly ? Math.max(0.6 * this.dimensionX, 480) : Math.max(0.3 * this.dimensionX, 480);
    const detailStillcutHeight = 0.7 * detailInfoboxWidth;

    const posterBorder = isMobileOnly ? 200 : 50;
    const detailPosterWidth = (detailInfoboxWidth - posterBorder) / 2;
    const detailPosterHeight = 1.5 * detailPosterWidth;

    return [detailInfoboxWidth, detailStillcutHeight, detailPosterWidth, detailPosterHeight]
  }
}

export default Resizer;
