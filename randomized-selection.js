const {secureShuffle} = require('./secure-random');

let randomizedSelectForRequestFunction = (input) => {
    let { peers } = input;
    let peerLimit = input.peerLimit;
    if (peers.length === 0) {
        return [];
    }
    if (peerLimit === undefined) {
        return secureShuffle(peers);
    }
    return secureShuffle(peers).slice(0, peerLimit);
};

let randomizedSelectForSendFunction = (input) => {
    let shuffledPeers = secureShuffle(input.peers);
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
  randomizedSelectForRequestFunction,
  randomizedSelectForSendFunction
};
