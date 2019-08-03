const shuffle = require('lodash.shuffle');

let defaultSelectForRequestFunction;
let defaultSelectForSendFunction;

function getAffectedModule(remoteActionName) {
  if (remoteActionName.indexOf(':') === -1) {
    return null;
  }
  return remoteActionName.split(':')[0];
}

function interchainSelectForRequest(input) {
  let {nodeInfo, peers, peerLimit, requestPacket} = input;

  let procedureTargetModule = getAffectedModule(requestPacket.procedure);
  if (procedureTargetModule) {
    let matchingPeers = peers.filter((peerInfo) => {
      return peerInfo.modules && peerInfo.modules[procedureTargetModule];
    });
    if (!matchingPeers.length) {
      return [];
    }
    let chosenPeer = matchingPeers[Math.floor(Math.random() * matchingPeers.length)];
    return [chosenPeer];
  }

  return defaultSelectForRequestFunction(input);
}

function interchainSelectForSend(input) {
  let {nodeInfo, peers, peerLimit, messagePacket} = input;

  let eventSourceModule = getAffectedModule(messagePacket.event);
  if (eventSourceModule) {
    let matchingPeers = peers.filter((peerInfo) => {
      return peerInfo.modules && peerInfo.modules[eventSourceModule];
    });
    if (!matchingPeers.length) {
      return [];
    }
    return shuffle(matchingPeers).slice(0, input.peerLimit);
  }

  return defaultSelectForSendFunction(input);
}

function attachInterchain(app) {
  let realLoadFunction = app.getModule('network').prototype.load;

  app.getModule('network').prototype.load = async function (channel) {
    await realLoadFunction.call(this, channel);
    let availableModules = Object.keys(app.getModules() || {}).reduce((modulesMap, moduleName) => {
      return {
        ...modulesMap,
        [moduleName]: {}
      };
    }, {});

    let realApplyNodeInfoFunction = this.network.p2p.applyNodeInfo;
    this.network.p2p.applyNodeInfo = function (nodeInfo) {
      let extendedNodeInfo = {
        ...nodeInfo,
        modules: availableModules
      };
      realApplyNodeInfoFunction.call(this, extendedNodeInfo);
    };

    defaultSelectForRequestFunction = this.network.p2p._peerPool._peerSelectForRequest;
    defaultSelectForSendFunction = this.network.p2p._peerPool._peerSelectForSend;

    this.network.p2p._peerPool._peerSelectForRequest = interchainSelectForRequest;
    this.network.p2p._peerPool._peerSelectForSend = interchainSelectForSend;
  };
}

module.exports = attachInterchain;
