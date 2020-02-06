const shuffle = require('lodash.shuffle');
const url = require('url');

const {
  randomizedSelectForConnectionFunction,
  randomizedSelectForRequestFunction,
  randomizedSelectForSendFunction
} = require('./randomized-selection');

function removeQueryString(string) {
  return string.replace(/\?.*$/, '');
}

function parseAction(remoteActionName) {
  if (remoteActionName.indexOf(':') === -1) {
    return {
      sanitizedAction: remoteActionName
    };
  }
  let remoteActionParts = remoteActionName.split(':');
  let routeString = remoteActionParts[0];

  let routeStringParts = routeString.split(',');
  let targetModule = removeQueryString(routeStringParts[0]);
  let sanitizedAction = `${targetModule}:${remoteActionParts[1]}`;

  return {
    routeString,
    sanitizedAction
  };
}

function getPeerModuleMatchScore(nodeInfo, peerInfo, moduleName) {
  let nodeModules = nodeInfo.modules;
  let peerModules = peerInfo.modules;
  if (!nodeModules) {
    return 0;
  }
  if (!peerModules) {
    return 0;
  }

  let nodeModuleData = nodeModules[moduleName];
  let peerModuleData = peerModules[moduleName];
  if (!nodeModuleData) {
    return 0;
  }
  if (!peerModuleData) {
    return 0;
  }

  return Object.keys(nodeModuleData).reduce((score, field) => {
    if (nodeModuleData[field] === peerModuleData[field]) {
      return score + 1;
    }
    return score;
  }, 0);
}

function doesPeerMatchRoute(peerInfo, routeString) {
  if (!routeString) {
    return true;
  }
  if (!peerInfo.modules) {
    return false;
  }
  let routeStringParts = routeString.split(',');
  for (let requirementString of routeStringParts) {
    let requirementParts;
    try {
      requirementParts = url.parse(requirementString, true);
    } catch (error) {
      return false;
    }
    let {pathname, query} = requirementParts;
    let moduleData = peerInfo.modules[pathname];
    if (!moduleData) {
      return false;
    }
    let peerHasAllRequiredModuleFields = Object.keys(query).every(
      (field) => {
        if (typeof moduleData[field] === 'number') {
          return moduleData[field] === Number(query[field]);
        }
        return moduleData[field] === query[field];
      }
    );
    if (!peerHasAllRequiredModuleFields) {
      return false;
    }
  }
  return true;
}

function interchainSelectForConnection(input) {
  let knownPeers = [...input.newPeers, ...input.triedPeers];
  let nodeInfo = this.nodeInfo || {};
  let nodeModulesList = Object.keys(nodeInfo.modules || {});

  let selectedPeers = randomizedSelectForConnectionFunction({
    ...input,
    nodeInfo: this.nodeInfo
  });

  let chosenPeersLookup = {};
  selectedPeers.forEach((peerInfo) => {
    chosenPeersLookup[`${peerInfo.ipAddress}:${peerInfo.wsPort}`] = true;
  });

  let matchingPeers = [];
  let maxPeersToAllocatePerModule = Math.ceil(input.peerLimit / nodeModulesList.length);

  nodeModulesList.forEach((moduleName) => {
    let matchingModulePeers = knownPeers
    .filter((peerInfo) => peerInfo.modules && peerInfo.modules[moduleName])
    .sort((peerInfoA, peerInfoB) => {
      let peerAScore = getPeerModuleMatchScore(nodeInfo, peerInfoA, moduleName);
      let peerBScore = getPeerModuleMatchScore(nodeInfo, peerInfoB, moduleName);
      if (peerAScore > peerBScore) {
        return -1;
      }
      if (peerAScore < peerBScore) {
        return 1;
      }
      return 0;
    })
    .slice(0, maxPeersToAllocatePerModule);
    matchingModulePeers.forEach((peerInfo) => {
      let peerId = `${peerInfo.ipAddress}:${peerInfo.wsPort}`;
      if (!chosenPeersLookup[peerId]) {
        chosenPeersLookup[peerId] = true;
        matchingPeers.push(peerInfo);
      }
    });
  });

  matchingPeers = shuffle(matchingPeers);

  let padPeersCount = selectedPeers.length - matchingPeers.length;
  let paddingPeers = [];

  // Pad the matchingPeers list with unknown peers to increase the chance of discovery.
  // This is useful for very small, newly created subnets.
  if (padPeersCount > 0) {
    let untriedPeers = shuffle(knownPeers.filter((peerInfo) => !peerInfo.protocolVersion));
    for (let i = 0; i < padPeersCount; i++) {
      let lastUntriedPeer = untriedPeers.pop();
      if (lastUntriedPeer) {
        let peerId = `${lastUntriedPeer.ipAddress}:${lastUntriedPeer.wsPort}`;
        if (!chosenPeersLookup[peerId]) {
          chosenPeersLookup[peerId] = true;
          paddingPeers.push(lastUntriedPeer);
        }
      }
    }
  }

  matchingPeers = paddingPeers.concat(matchingPeers);

  let regularPeerSelectionProbability = 1 / (nodeModulesList.length + 1);

  selectedPeers = selectedPeers.map((defaultPeer) => {
    if (Math.random() > regularPeerSelectionProbability) {
      let lastMatchingPeer = matchingPeers.pop();
      if (lastMatchingPeer) {
        return lastMatchingPeer;
      }
      return defaultPeer;
    }
    return defaultPeer;
  });

  return selectedPeers;
}

function interchainSelectForRequest(input) {
  let {nodeInfo, peers, peerLimit, requestPacket} = input;

  let {routeString, sanitizedAction} = parseAction(requestPacket.procedure);
  requestPacket.procedure = sanitizedAction;

  if (routeString) {
    let matchingPeers = peers.filter((peerInfo) => doesPeerMatchRoute(peerInfo, routeString));
    if (!matchingPeers.length) {
      return [];
    }
    return randomizedSelectForRequestFunction({
      ...input,
      peers: matchingPeers
    });
  }

  return randomizedSelectForRequestFunction(input);
}

function interchainSelectForSend(input) {
  let {nodeInfo, peers, peerLimit, messagePacket} = input;

  let {routeString, sanitizedAction} = parseAction(messagePacket.event);
  messagePacket.event = sanitizedAction;

  if (routeString) {
    let matchingPeers = peers.filter((peerInfo) => doesPeerMatchRoute(peerInfo, routeString));
    if (!matchingPeers.length) {
      return [];
    }
    return randomizedSelectForSendFunction({
      ...input,
      peers: matchingPeers
    });
  }

  return randomizedSelectForSendFunction(input);
}

module.exports = {
  peerSelectionForConnection: interchainSelectForConnection,
  peerSelectionForRequest: interchainSelectForRequest,
  peerSelectionForSend: interchainSelectForSend
};
