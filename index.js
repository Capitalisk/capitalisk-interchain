const shuffle = require('lodash.shuffle');
const url = require('url');

const {
  randomizedSelectForRequestFunction,
  randomizedSelectForSendFunction
} = require('./randomized-selection');

const PEER_KIND_OUTBOUND = 'outbound';

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
  let disconnectedKnownPeers = [...input.disconnectedNewPeers, ...input.disconnectedTriedPeers];
  let connectedKnownPeers = [...input.connectedNewPeers, ...input.connectedTriedPeers];
  let nodeInfo = input.nodeInfo || {};
  let nodeModulesList = Object.keys(nodeInfo.modules || {}).filter((moduleName) => moduleName != null);
  let maxPeersToAllocatePerModule = input.maxOutboundPeerCount;

  let disconnectedModulePeerMap = {};
  let moduleQuotas = [];
  nodeModulesList.forEach((moduleName) => {
    let disconnectedModulePeers = disconnectedKnownPeers
      .filter((peerInfo) => peerInfo.modules && peerInfo.modules[moduleName])
      .sort((peerInfoA, peerInfoB) => {
        let peerAScore = getPeerModuleMatchScore(nodeInfo, peerInfoA, moduleName);
        let peerBScore = getPeerModuleMatchScore(nodeInfo, peerInfoB, moduleName);
        if (peerAScore > peerBScore) {
          return 1;
        }
        if (peerAScore < peerBScore) {
          return -1;
        }
        return Math.random() < .5 ? -1 : 1;
      })
      .slice(0, maxPeersToAllocatePerModule);

    disconnectedModulePeerMap[moduleName] = disconnectedModulePeers;

    let outboundConnectedModulePeers = connectedKnownPeers
      .filter((peerInfo) => peerInfo.kind === PEER_KIND_OUTBOUND && peerInfo.modules && peerInfo.modules[moduleName]);

    moduleQuotas.push({
      moduleName,
      quota: maxPeersToAllocatePerModule - outboundConnectedModulePeers.length
    });
  });

  let filterUnrelatedPeers = (peerInfo) => {
    if (!peerInfo.modules) {
      return true;
    }
    return nodeModulesList.every((moduleName) => !peerInfo.modules[moduleName]);
  };

  let disconnectedUnrelatedPeers = shuffle(disconnectedKnownPeers.filter(filterUnrelatedPeers));

  let outboundConnectedUnrelatedPeers = connectedKnownPeers
    .filter((peerInfo) => peerInfo.kind === PEER_KIND_OUTBOUND)
    .filter(filterUnrelatedPeers);

  moduleQuotas.push({
    moduleName: null,
    quota: maxPeersToAllocatePerModule - outboundConnectedUnrelatedPeers.length
  });

  let sortModuleQuotas = (quotaA, quotaB) => {
    if (quotaA.quota > quotaB.quota) {
      return 1;
    }
    if (quotaA.quota < quotaB.quota) {
      return -1;
    }
    return Math.random() < .5 ? -1 : 1;
  };

  moduleQuotas.sort(sortModuleQuotas);

  let peerLimit = (input.maxOutboundPeerCount * moduleQuotas.length) - input.outboundPeerCount;
  let selectedPeerMap = new Map();

  while (selectedPeerMap.size < peerLimit) {
    let topModuleQuota = moduleQuotas[moduleQuotas.length - 1];
    if (topModuleQuota.quota < 1) {
      break;
    }
    let targetPeers;
    if (topModuleQuota.moduleName === null) {
      targetPeers = disconnectedUnrelatedPeers;
    } else {
      targetPeers = disconnectedModulePeerMap[topModuleQuota.moduleName];
    }
    if (targetPeers.length) {
      let peerInfo = targetPeers.pop();
      let peerId = `${peerInfo.ipAddress}:${peerInfo.wsPort}`;
      if (!selectedPeerMap.has(peerId)) {
        selectedPeerMap.set(peerId, peerInfo);
        topModuleQuota.quota--;
        moduleQuotas.sort(sortModuleQuotas);
      }
    } else {
      moduleQuotas.pop();
      if (!moduleQuotas.length) {
        break;
      }
    }
  }

  return [...selectedPeerMap.values()];
}

function selectUniqueMatchingPeersByIp(peerList, routeString) {
  let uniquePeerGroups = {};
  for (let peerInfo of peerList) {
    if (!doesPeerMatchRoute(peerInfo, routeString)) {
      continue;
    }
    if (!uniquePeerGroups[peerInfo.ipAddress]) {
      uniquePeerGroups[peerInfo.ipAddress] = [];
    }
    uniquePeerGroups[peerInfo.ipAddress].push(peerInfo);
  }
  let uniquePeerGroupList = Object.values(uniquePeerGroups);
  return uniquePeerGroupList.map((peerGroup) => {
    let randomIndex = Math.floor(Math.random() * peerGroup.length);
    return peerGroup[randomIndex];
  });
}

function interchainSelectForRequest(input) {
  let {peers, peerLimit, requestPacket} = input;

  let {routeString, sanitizedAction} = parseAction(requestPacket.procedure);
  requestPacket.procedure = sanitizedAction;

  if (routeString) {
    let matchingPeers;
    let outboundPeers = peers.filter((peerInfo) => peerInfo.kind === PEER_KIND_OUTBOUND);
    let outboundMatchingPeers = selectUniqueMatchingPeersByIp(outboundPeers, routeString);
    let unmetPeerQuota = peerLimit - outboundMatchingPeers.length;
    if (unmetPeerQuota > 0) {
      let inboundPeers = peers.filter((peerInfo) => peerInfo.kind !== PEER_KIND_OUTBOUND);
      let inboundMatchingPeers = selectUniqueMatchingPeersByIp(inboundPeers, routeString);
      let randomInboundMatchingPeers = shuffle(inboundMatchingPeers).slice(0, unmetPeerQuota);
      matchingPeers = [
        ...outboundMatchingPeers,
        ...randomInboundMatchingPeers
      ];
    } else {
      matchingPeers = outboundMatchingPeers;
    }
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
  let {peers, messagePacket} = input;

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
