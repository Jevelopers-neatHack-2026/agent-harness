import https from 'node:https';
import http from 'node:http';
import net from 'node:net';
import tls from 'node:tls';
import dns from 'node:dns';
import { Resolver } from 'node:dns/promises';
import { recordSyntheticMetric } from './synthetic-metrics.js';

function blockUncontrolledNetwork() {
  recordSyntheticMetric('blocked_external_network_attempt');
  throw new Error('Uncontrolled JEV network access is disabled in offline tests.');
}

https.request = blockUncontrolledNetwork;
https.get = blockUncontrolledNetwork;
http.request = blockUncontrolledNetwork;
http.get = blockUncontrolledNetwork;
net.connect = blockUncontrolledNetwork;
net.createConnection = blockUncontrolledNetwork;
tls.connect = blockUncontrolledNetwork;
dns.lookup = blockUncontrolledNetwork;
dns.resolve = blockUncontrolledNetwork;
dns.resolve4 = blockUncontrolledNetwork;
dns.resolve6 = blockUncontrolledNetwork;
dns.promises.lookup = blockUncontrolledNetwork;
dns.promises.resolve4 = blockUncontrolledNetwork;
dns.promises.resolve6 = blockUncontrolledNetwork;
Resolver.prototype.resolve4 = blockUncontrolledNetwork;
Resolver.prototype.resolve6 = blockUncontrolledNetwork;
globalThis.fetch = blockUncontrolledNetwork;
