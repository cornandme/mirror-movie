class Resizer {
  getBannerImageHeight = (dimensionX, dimensionY) => {
    const bannerImageHeight = 0.7 * dimensionX;
    return Math.min(dimensionY, bannerImageHeight) - 50;
  };

  getMovieListBoardMove = (bannerImageHeight) => {
    return 0.25 * bannerImageHeight;
  };

  getFrontPosterCount = (dimensionX) => {
    const frontPosterCount = Math.round(Math.log(dimensionX) / Math.log(1.3) - 20);
    return frontPosterCount;
  };

  getFrontPosterSize = (dimensionX, frontPosterCount) => {
    const room = dimensionX - 50 - frontPosterCount * 10;
    const frontPosterWidth = room / frontPosterCount;
    const frontPosterHeight = 1.5 * frontPosterWidth;
    return [frontPosterWidth, frontPosterHeight];
  };

  getSearchStillcutSize = (dimensionX) => {
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
}

export default Resizer;
