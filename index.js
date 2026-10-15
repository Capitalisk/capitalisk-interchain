const {
  secureRandomInt,
  secureRandomBoolean,
  secureShuffle
} = require('./secure-random');

const {
  randomizedSelectForRequestFunction,
  randomizedSelectForSendFunction
} = require('./randomized-selection');

const PEER_KIND_OUTBOUND = 'outbound';

// Caps so that one host cannot claim every outbound slot and eclipse the node.
const DEFAULT_MAX_OUTBOUND_PEERS_PER_IP_ADDRESS = 2;
const DEFAULT_MAX_OUTBOUND_PEERS_PER_SUBNET = 3;

// Module and field names are remote-supplied, so re-key them into a
// null-prototype map: __proto__ or toString must not resolve to a built-in.
function toSafeMap(object) {
  if (object == null || typeof object !== 'object') {
    return Object.create(null);
  }
  return Object.assign(Object.create(null), object);
}

function getPeerModules(peerInfo) {
  return toSafeMap(peerInfo && peerInfo.modules);
}

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

  let nodeModuleData = toSafeMap(nodeModules)[moduleName];
  let peerModuleData = toSafeMap(peerModules)[moduleName];
  if (!nodeModuleData) {
    return 0;
  }
  if (!peerModuleData) {
    return 0;
  }

  let safePeerModuleData = toSafeMap(peerModuleData);

  return Object.keys(nodeModuleData).reduce((score, field) => {
    if (nodeModuleData[field] === safePeerModuleData[field]) {
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

function parseRouteRequirement(requirementString) {
  let [pathAndQuery] = requirementString.split('#');
  let queryStringIndex = pathAndQuery.indexOf('?');
  if (queryStringIndex === -1) {
    return {
      pathname: pathAndQuery,
      query: Object.create(null)
    };
  }
  let pathname = pathAndQuery.slice(0, queryStringIndex);
  let searchParams = new URLSearchParams(pathAndQuery.slice(queryStringIndex + 1));
  let query = Object.create(null);
  for (let [field, value] of searchParams) {
    let existingValue = query[field];
    if (existingValue === undefined) {
      query[field] = value;
    } else if (Array.isArray(existingValue)) {
      existingValue.push(value);
    } else {
      query[field] = [existingValue, value];
    }
  }
  return {pathname, query};
}

function doesPeerMatchRoute(peerInfo, routeString) {
  if (!routeString) {
    return true;
  }
  if (!peerInfo.modules) {
    return false;
  }
  let peerModules = getPeerModules(peerInfo);
  let routeStringParts = routeString.split(',');
  for (let requirementString of routeStringParts) {
    let {pathname, query} = parseRouteRequirement(requirementString);
    let moduleData = peerModules[pathname];
    if (!moduleData) {
      return false;
    }
    let safeModuleData = toSafeMap(moduleData);
    let peerHasRequiredModuleFields;

    let {match, ...plainQuery} = query;
    if (match === 'or') {
      peerHasRequiredModuleFields = Object.keys(plainQuery).some(
        (field) => {
          let moduleValue = safeModuleData[field];
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
          return doValuesMatch(safeModuleData[field], plainQuery[field]);
        }
      );
    }

    if (!peerHasRequiredModuleFields) {
      return false;
    }
  }
  return true;
}

function expandIpv6Address(ipAddress) {
  let address = ipAddress.replace(/^\[/, '').replace(/\]$/, '').replace(/%.*$/, '');
  // A trailing IPv4 part (as in ::ffff:1.2.3.4) is worth two hextets.
  let ipv4SuffixMatch = address.match(/(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (ipv4SuffixMatch) {
    let octets = ipv4SuffixMatch.slice(1).map(Number);
    if (octets.some((octet) => octet > 255)) {
      return null;
    }
    let highHextet = ((octets[0] << 8) | octets[1]).toString(16);
    let lowHextet = ((octets[2] << 8) | octets[3]).toString(16);
    address = `${address.slice(0, ipv4SuffixMatch.index)}${highHextet}:${lowHextet}`;
  }
  let compressedParts = address.split('::');
  if (compressedParts.length > 2) {
    return null;
  }
  let headHextets = compressedParts[0] ? compressedParts[0].split(':') : [];
  let tailHextets = compressedParts.length === 2 && compressedParts[1] ?
    compressedParts[1].split(':') : [];
  let hextets;
  if (compressedParts.length === 2) {
    let zeroFillLength = 8 - headHextets.length - tailHextets.length;
    if (zeroFillLength < 0) {
      return null;
    }
    hextets = [...headHextets, ...new Array(zeroFillLength).fill('0'), ...tailHextets];
  } else {
    hextets = headHextets;
  }
  if (hextets.length !== 8) {
    return null;
  }
  if (!hextets.every((hextet) => /^[0-9a-f]{1,4}$/.test(hextet))) {
    return null;
  }
  return hextets.map((hextet) => hextet.padStart(4, '0'));
}

// Groups an address for the per-subnet cap: /24 for IPv4, /64 for IPv6.
// Normalized first so a peer cannot evade the cap by varying notation.
function getPeerSubnetKey(ipAddress) {
  if (typeof ipAddress !== 'string') {
    return `invalid:${String(ipAddress)}`;
  }
  let address = ipAddress.trim().toLowerCase();
  // Unwrap IPv4-mapped IPv6 so it is capped as the IPv4 address it is.
  let ipv4MappedMatch = address.match(
    /^\[?::(?:ffff:)?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\]?$/
  );
  if (ipv4MappedMatch) {
    address = ipv4MappedMatch[1];
  }
  let ipv4Parts = address.split('.');
  let isIpv4Address = ipv4Parts.length === 4 &&
    ipv4Parts.every((part) => /^\d{1,3}$/.test(part) && Number(part) < 256);
  if (isIpv4Address) {
    return `v4:${ipv4Parts.slice(0, 3).join('.')}`;
  }
  if (address.includes(':')) {
    let hextets = expandIpv6Address(address);
    if (hextets) {
      return `v6:${hextets.slice(0, 4).join(':')}`;
    }
  }
  // Unrecognized format: cap the exact string so it is still limited.
  return `other:${address}`;
}

function incrementCount(counts, key) {
  counts.set(key, (counts.get(key) || 0) + 1);
}

// Takes up to maxPeersToTake peers, holding to the caps. Needed here as well as
// at selection time: the pool is truncated, so a flood sharing one IP or subnet
// would crowd honest peers out of it and starve the node of connections.
function takeDiversePeers(peerList, maxPeersToTake, maxPeersPerIpAddress, maxPeersPerSubnet) {
  let takenPeers = [];
  let ipAddressCounts = new Map();
  let subnetCounts = new Map();
  for (let peerInfo of peerList) {
    if (takenPeers.length >= maxPeersToTake) {
      break;
    }
    let subnetKey = getPeerSubnetKey(peerInfo.ipAddress);
    let isWithinIpAddressLimit = (ipAddressCounts.get(peerInfo.ipAddress) || 0) < maxPeersPerIpAddress;
    let isWithinSubnetLimit = (subnetCounts.get(subnetKey) || 0) < maxPeersPerSubnet;
    if (!isWithinIpAddressLimit || !isWithinSubnetLimit) {
      continue;
    }
    takenPeers.push(peerInfo);
    incrementCount(ipAddressCounts, peerInfo.ipAddress);
    incrementCount(subnetCounts, subnetKey);
  }
  return takenPeers;
}

function interchainSelectForConnection(input) {
  let disconnectedKnownPeers = [...input.disconnectedNewPeers, ...input.disconnectedTriedPeers];
  let connectedKnownPeers = [...input.connectedNewPeers, ...input.connectedTriedPeers];
  let nodeInfo = input.nodeInfo || {};
  let nodeModulesList = Object.keys(nodeInfo.modules || {}).filter((moduleName) => moduleName != null);
  let maxPeersToAllocatePerModule = input.maxOutboundPeerCount;
  let maxPeersPerIpAddress = input.maxOutboundPeersPerIpAddress == null ?
    DEFAULT_MAX_OUTBOUND_PEERS_PER_IP_ADDRESS : input.maxOutboundPeersPerIpAddress;
  let maxPeersPerSubnet = input.maxOutboundPeersPerSubnet == null ?
    DEFAULT_MAX_OUTBOUND_PEERS_PER_SUBNET : input.maxOutboundPeersPerSubnet;

  let hasPeerModule = (peerInfo, moduleName) => Boolean(getPeerModules(peerInfo)[moduleName]);

  let disconnectedModulePeerMap = Object.create(null);
  let moduleQuotas = [];
  nodeModulesList.forEach((moduleName) => {
    let maxSimilarPeersToAllocate = Math.round(maxPeersToAllocatePerModule / 2);
    let disconnectedModulePeers = disconnectedKnownPeers
      .filter((peerInfo) => hasPeerModule(peerInfo, moduleName));
    // Shuffle, then stable-sort by score, so equal scores land in a uniformly
    // random order. A random comparator is non-transitive and biases towards
    // whichever peer was listed first, which peer discovery can influence.
    let sortedDisconnectedModulePeers = secureShuffle(disconnectedModulePeers)
      .map((peerInfo) => ({
        peerInfo,
        score: getPeerModuleMatchScore(nodeInfo, peerInfo, moduleName)
      }))
      .sort((entryA, entryB) => entryB.score - entryA.score)
      .map((entry) => entry.peerInfo);
    let mostSimilarDisconnectedModulePeers = takeDiversePeers(
      sortedDisconnectedModulePeers,
      maxSimilarPeersToAllocate,
      maxPeersPerIpAddress,
      maxPeersPerSubnet
    );
    let mostSimilarPeerSet = new Set(mostSimilarDisconnectedModulePeers);
    let remainingModulePeers = secureShuffle(
      sortedDisconnectedModulePeers.filter((peerInfo) => !mostSimilarPeerSet.has(peerInfo))
    );
    let remainingPeersToAllocate = maxPeersToAllocatePerModule - mostSimilarDisconnectedModulePeers.length;
    let randomModulePeers = takeDiversePeers(
      remainingModulePeers,
      remainingPeersToAllocate,
      maxPeersPerIpAddress,
      maxPeersPerSubnet
    );

    disconnectedModulePeerMap[moduleName] = [...mostSimilarDisconnectedModulePeers, ...randomModulePeers];

    let outboundConnectedModulePeers = connectedKnownPeers
      .filter((peerInfo) => peerInfo.kind === PEER_KIND_OUTBOUND && hasPeerModule(peerInfo, moduleName));

    moduleQuotas.push({
      moduleName,
      quota: maxPeersToAllocatePerModule - outboundConnectedModulePeers.length
    });
  });

  let filterUnrelatedPeers = (peerInfo) => {
    let peerModules = getPeerModules(peerInfo);
    return nodeModulesList.every((moduleName) => !peerModules[moduleName]);
  };

  let disconnectedUnrelatedPeers = takeDiversePeers(
    secureShuffle(disconnectedKnownPeers.filter(filterUnrelatedPeers)),
    maxPeersToAllocatePerModule,
    maxPeersPerIpAddress,
    maxPeersPerSubnet
  );

  let outboundConnectedPeers = connectedKnownPeers
    .filter((peerInfo) => peerInfo.kind === PEER_KIND_OUTBOUND);

  let outboundConnectedUnrelatedPeers = outboundConnectedPeers
    .filter(filterUnrelatedPeers);

  moduleQuotas.push({
    moduleName: null,
    quota: maxPeersToAllocatePerModule - outboundConnectedUnrelatedPeers.length
  });

  let sortModuleQuotas = () => {
    // Shuffle-then-stable-sort as above. Ascending, so the neediest is last.
    moduleQuotas = secureShuffle(moduleQuotas)
      .sort((quotaA, quotaB) => quotaA.quota - quotaB.quota);
  };

  sortModuleQuotas();

  // Seed the caps with existing connections, else a host accumulates slots
  // across rounds.
  let ipAddressCounts = new Map();
  let subnetCounts = new Map();

  for (let peerInfo of outboundConnectedPeers) {
    incrementCount(ipAddressCounts, peerInfo.ipAddress);
    incrementCount(subnetCounts, getPeerSubnetKey(peerInfo.ipAddress));
  }

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
      let subnetKey = getPeerSubnetKey(peerInfo.ipAddress);
      let isWithinIpAddressLimit = (ipAddressCounts.get(peerInfo.ipAddress) || 0) < maxPeersPerIpAddress;
      let isWithinSubnetLimit = (subnetCounts.get(subnetKey) || 0) < maxPeersPerSubnet;
      if (!selectedPeerMap.has(peerId) && isWithinIpAddressLimit && isWithinSubnetLimit) {
        selectedPeerMap.set(peerId, peerInfo);
        incrementCount(ipAddressCounts, peerInfo.ipAddress);
        incrementCount(subnetCounts, subnetKey);
        topModuleQuota.quota--;
        sortModuleQuotas();
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
  // Null-prototype: a remote-supplied ipAddress of __proto__ or constructor
  // must not resolve to an inherited built-in.
  let uniquePeerGroups = Object.create(null);
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
    let randomIndex = secureRandomInt(peerGroup.length);
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
  // Null-prototype: a remote-supplied __proto__ key would otherwise repoint
  // this map's prototype at an attacker-controlled peer.
  let uniquePeers = Object.create(null);
  for (let peer of peers) {
    if (!uniquePeers[peer.ipAddress] || secureRandomBoolean()) {
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
