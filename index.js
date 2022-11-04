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

function doValuesMatch(moduleValue, queryValue) {
  if (typeof moduleValue === 'number') {
    return moduleValue === Number(queryValue);
  }
  return moduleValue === queryValue;
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
    let peerHasRequiredModuleFields;

    let {match, ...plainQuery} = query;
    if (match === 'or') {
      peerHasRequiredModuleFields = Object.keys(plainQuery).some(
        (field) => {
          let moduleValue = moduleData[field];
          let queryValue = plainQuery[field];
          if (Array.isArray(queryValue)) {
            return queryValue.some((value) => doValuesMatch(moduleValue, value));
          }
          return doValuesMatch(moduleValue, queryValue);
        }
      );
    } else {
      peerHasRequiredModuleFields = Object.keys(plainQuery).every(
        (field) => {
          return doValuesMatch(moduleData[field], plainQuery[field]);
        }
      );
    }

    if (!peerHasRequiredModuleFields) {
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
    let maxSimilarPeersToAllocate = Math.round(maxPeersToAllocatePerModule / 2);
    let disconnectedModulePeers = disconnectedKnownPeers
      .filter((peerInfo) => peerInfo.modules && peerInfo.modules[moduleName]);
    let sortedDisconnectedModulePeers = disconnectedModulePeers
      .sort((peerInfoA, peerInfoB) => {
        let peerAScore = getPeerModuleMatchScore(nodeInfo, peerInfoA, moduleName);
        let peerBScore = getPeerModuleMatchScore(nodeInfo, peerInfoB, moduleName);
        if (peerAScore > peerBScore) {
          return -1;
        }
        if (peerAScore < peerBScore) {
          return 1;
        }
        return Math.random() < .5 ? -1 : 1;
      });
    let mostSimilarDisconnectedModulePeers = sortedDisconnectedModulePeers
      .slice(0, maxSimilarPeersToAllocate);
    let remainingModulePeers = shuffle(sortedDisconnectedModulePeers.slice(maxSimilarPeersToAllocate));
    let remainingPeersToAllocate = maxPeersToAllocatePerModule - maxSimilarPeersToAllocate;
    let randomModulePeers = remainingModulePeers.slice(0, remainingPeersToAllocate);

    disconnectedModulePeerMap[moduleName] = [...mostSimilarDisconnectedModulePeers, ...randomModulePeers];

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
  let {peers, requestPacket} = input;

  let {routeString, sanitizedAction} = parseAction(requestPacket.procedure);
  requestPacket.procedure = sanitizedAction;

  let outboundPeers = peers.filter((peerInfo) => peerInfo.kind === PEER_KIND_OUTBOUND);

  if (routeString) {
    let matchingPeers = selectUniqueMatchingPeersByIp(outboundPeers, routeString);
    if (!matchingPeers.length) {
      return [];
    }
    return randomizedSelectForRequestFunction({
      ...input,
      peers: matchingPeers
    });
  }

  return randomizedSelectForRequestFunction({
    ...input,
    peers: outboundPeers
  });
}

function interchainSelectForSend(input) {
  let {peers, messagePacket} = input;
  let uniquePeers = {};
  for (let peer of peers) {
    if (!uniquePeers[peer.ipAddress] || Math.random() < .5) {
      uniquePeers[peer.ipAddress] = peer;
    }
  }
  let uniquePeerList = Object.values(uniquePeers);

  let {routeString, sanitizedAction} = parseAction(messagePacket.event);
  messagePacket.event = sanitizedAction;

  if (routeString) {
    let matchingPeers = uniquePeerList.filter((peerInfo) => doesPeerMatchRoute(peerInfo, routeString));
    if (!matchingPeers.length) {
      return [];
    }
    return randomizedSelectForSendFunction({
      ...input,
      peers: matchingPeers
    });
  }

  return randomizedSelectForSendFunction({
    ...input,
    peers: uniquePeerList
  });
}

module.exports = {
  peerSelectionForConnection: interchainSelectForConnection,
  peerSelectionForRequest: interchainSelectForRequest,
  peerSelectionForSend: interchainSelectForSend
};
