import { isMobileOnly, isTablet } from 'react-device-detect';

class Resizer {
  constructor() {
    this.isMobileOnly = isMobileOnly;
    this.minWidth = 280;
    this.mobileResize()
  }

  mobileResize = () => {
    this.isMobile = isMobileOnly || (this.dimensionX <= 1080);
    this.isTablet = isTablet;
    this.isMobileOrTablet = this.isMobile || this.isTablet;

    this.nav = {
      navBarPadding: this.isMobile ? '0 1.5rem 0 1.5rem' : '0 2.5rem 0 2.5rem',
      searchInputWidth: this.isMobile ? '40%' : '20rem'
    }

    this.movies = {
      topicAreaPadding: this.isMobile ? '2rem 0.5rem 1rem 1.5rem' : '2rem 1.5rem 1rem 2.5rem',
      topicTitleFontSize: this.isMobile ? '1.2rem' : '1.5rem'
    }

    this.searchResult = {
      modalBlockFlexDirection: this.isMobile ? 'column' : 'row',
      similarWordBlockMarginTop: this.isMobile ? '7rem' : 0,
      stillcutMovieTitleBottom: this.isMobile ? '2rem' : '3rem'
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
    const dimensionX = this.isMobile ? 1.5 * this.dimensionX : this.dimensionX;
    this.frontPosterCount = Math.round(Math.log(dimensionX) / Math.log(1.3) - 20);
    return this.frontPosterCount;
  };

  getFrontPosterSize = () => {
    const border = this.isMobile ? 20 : 40;
    const posterMargin = 10;
    const room = this.dimensionX - border - this.frontPosterCount * posterMargin;

    const frontPosterWidth = room / this.frontPosterCount;
    const frontPosterHeight = 1.5 * frontPosterWidth;

    return [frontPosterWidth, frontPosterHeight];
  };

  getSearchStillcutSize = () => {
    const border = this.isMobile ? 40 : 310;
    const room = this.dimensionX - border;
    let searchStillcutWidth;
    if (this.dimensionX <= 875) {
      searchStillcutWidth = room / 2;
    } else if (this.dimensionX <= 1080) {
      searchStillcutWidth = room / 3;
    } else if (this.dimensionX <= 1548) {
      searchStillcutWidth = room / 3;
    } else if (this.dimensionX <= 1972) {
      searchStillcutWidth = room / 4;
    } else if (this.dimensionX <= 2400) {
      searchStillcutWidth = room / 5;
    } else {
      searchStillcutWidth = room / 6;
    }

    const searchStillcutHeight = 0.6 * searchStillcutWidth;
    return [searchStillcutWidth, searchStillcutHeight];
  };

  getDetailInfoboxSize = () => {
    const infoBoxRatio = this.isMobile ? 0.9 : this.isTablet ? 0.6 : 0.3;
    const detailInfoboxWidth = Math.max(infoBoxRatio * this.dimensionX, this.minWidth);
    const detailStillcutHeight = 0.7 * detailInfoboxWidth;

    const posterBorder = 60;
    const detailPosterWidth = (detailInfoboxWidth - posterBorder) / 3;
    const detailPosterHeight = 1.5 * detailPosterWidth;

    return [detailInfoboxWidth, detailStillcutHeight, detailPosterWidth, detailPosterHeight]
  }
}

export default Resizer;
