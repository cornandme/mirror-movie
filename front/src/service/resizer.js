class Resizer {
  getFrontPosterCount = (dimensionX) => {
    const frontPosterCount = Math.round(Math.log(dimensionX) / Math.log(1.3) - 20);
    console.log(frontPosterCount);
    return frontPosterCount;
  };

  getFrontPosterSize = (dimensionX, frontPosterCount) => {
    const room = dimensionX - 50 - frontPosterCount * 10;
    const frontPosterWidth = room / frontPosterCount;
    const frontPosterHeight = 1.3 * frontPosterWidth;
    return [frontPosterWidth, frontPosterHeight];
  };
}

export default Resizer;
