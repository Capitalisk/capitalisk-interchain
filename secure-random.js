const crypto = require('crypto');

// Peer selection is a security boundary: a predictable or biased draw lets an
// attacker steer a node's connections, so Math.random is not good enough.
function secureRandomInt(maxExclusive) {
  if (maxExclusive <= 1) {
    return 0;
  }
  return crypto.randomInt(maxExclusive);
}

function secureRandomBoolean() {
  return secureRandomInt(2) === 0;
}

// Uniform Fisher-Yates. Unlike a random sort comparator, this is transitive,
// so every permutation stays equally likely.
function secureShuffle(list) {
  let shuffledList = [...list];
  for (let i = shuffledList.length - 1; i > 0; i--) {
    let j = secureRandomInt(i + 1);
    let currentItem = shuffledList[i];
    shuffledList[i] = shuffledList[j];
    shuffledList[j] = currentItem;
  }
  return shuffledList;
}

module.exports = {
  secureRandomInt,
  secureRandomBoolean,
  secureShuffle
};
