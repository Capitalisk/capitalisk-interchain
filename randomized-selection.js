const shuffle = require('lodash.shuffle');

let randomizedSelectForConnectionFunction = (input) => {
    if (input.peerLimit && input.peerLimit < 0) {
        return [];
    }
    if (input.peerLimit === undefined ||
        input.peerLimit >= input.triedPeers.length + input.newPeers.length) {
        return [...input.newPeers, ...input.triedPeers];
    }
    if (input.triedPeers.length === 0 && input.newPeers.length === 0) {
        return [];
    }
    let x = input.triedPeers.length / (input.triedPeers.length + input.newPeers.length);
    let minimumProbability = 0.5;
    let r = Math.max(x, minimumProbability);
    let shuffledTriedPeers = shuffle(input.triedPeers);
    let shuffledNewPeers = shuffle(input.newPeers);
    return [...Array(input.peerLimit)].map(() => {
        if (shuffledTriedPeers.length !== 0) {
            if (Math.random() < r) {
                return shuffledTriedPeers.pop();
            }
        }
        if (shuffledNewPeers.length !== 0) {
            return shuffledNewPeers.pop();
        }
        return shuffledTriedPeers.pop();
    });
};

let randomizedSelectForRequestFunction = (input) => {
    let { peers } = input;
    let peerLimit = input.peerLimit;
    if (peers.length === 0) {
        return [];
    }
    if (peerLimit === undefined) {
        return shuffle(peers);
    }
    return shuffle(peers).slice(0, peerLimit);
};

let randomizedSelectForSendFunction = (input) => {
    let shuffledPeers = shuffle(input.peers);
    let peerLimit = input.peerLimit;
    let halfPeerLimit = Math.round(peerLimit / 2);
    let outboundPeers = shuffledPeers.filter((peerInfo) => peerInfo.kind === 'outbound');
    let inboundPeers = shuffledPeers.filter((peerInfo) => peerInfo.kind === 'inbound');
    let shortestPeersList;
    let longestPeersList;
    if (outboundPeers.length < inboundPeers.length) {
        shortestPeersList = outboundPeers;
        longestPeersList = inboundPeers;
    }
    else {
        shortestPeersList = inboundPeers;
        longestPeersList = outboundPeers;
    }
    let selectedFirstKindPeers = shortestPeersList.slice(0, halfPeerLimit);
    let remainingPeerLimit = peerLimit - selectedFirstKindPeers.length;
    let selectedSecondKindPeers = longestPeersList.slice(0, remainingPeerLimit);
    return selectedFirstKindPeers.concat(selectedSecondKindPeers);
};

module.exports = {
  randomizedSelectForConnectionFunction,
  randomizedSelectForRequestFunction,
  randomizedSelectForSendFunction
};
