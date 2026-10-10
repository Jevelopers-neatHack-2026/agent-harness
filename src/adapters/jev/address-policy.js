import { isIP } from 'node:net';

function ipv4Octets(address) {
  if (isIP(address) !== 4) return null;
  const octets = address.split('.').map(Number);
  return octets.length === 4 && octets.every(value => Number.isInteger(value) && value >= 0 && value <= 255)
    ? octets
    : null;
}

function ipv4GloballyRoutable(address) {
  const octets = ipv4Octets(address);
  if (!octets) return false;
  const [a, b, c] = octets;
  if (a === 0 || a === 10 || a === 127 || a >= 224) return false;
  if (a === 100 && b >= 64 && b <= 127) return false;
  if (a === 169 && b === 254) return false;
  if (a === 172 && b >= 16 && b <= 31) return false;
  if (a === 192 && b === 0 && c === 0) return false;
  if (a === 192 && b === 0 && c === 2) return false;
  if (a === 192 && b === 88 && c === 99) return false;
  if (a === 192 && b === 168) return false;
  if (a === 198 && (b === 18 || b === 19)) return false;
  if (a === 198 && b === 51 && c === 100) return false;
  if (a === 203 && b === 0 && c === 113) return false;
  return true;
}

function ipv6Groups(address) {
  if (isIP(address) !== 6 || address.includes('%') || address.includes('.')) return null;
  const halves = address.toLowerCase().split('::');
  if (halves.length > 2) return null;
  const parseHalf = half => half === '' ? [] : half.split(':').map(group => {
    if (!/^[0-9a-f]{1,4}$/u.test(group)) return null;
    return Number.parseInt(group, 16);
  });
  const left = parseHalf(halves[0]);
  const right = halves.length === 2 ? parseHalf(halves[1]) : [];
  if (left.some(value => value === null) || right.some(value => value === null)) return null;
  if (halves.length === 1) return left.length === 8 ? left : null;
  if (left.length + right.length >= 8) return null;
  return [...left, ...Array(8 - left.length - right.length).fill(0), ...right];
}

function ipv6GloballyRoutable(address) {
  const groups = ipv6Groups(address);
  if (!groups) return false;
  const first = groups[0];
  // Only the global unicast 2000::/3 range is accepted. This excludes the
  // unspecified, loopback, mapped, NAT64, unique-local, link-local and multicast ranges.
  if (first < 0x2000 || first > 0x3fff) return false;
  if (first === 0x2001 && groups[1] <= 0x01ff) return false; // IETF special-purpose block, including documentation.
  if (first === 0x2001 && groups[1] === 0x0db8) return false; // Documentation prefix.
  if (first === 0x2002) return false; // 6to4 embeds an IPv4 destination.
  if ((groups[4] === 0x0000 || groups[4] === 0x0200) && groups[5] === 0x5efe) return false; // ISATAP embeds an IPv4 destination.
  if (first === 0x3fff && groups[1] <= 0x0fff) return false; // documentation range.
  return true;
}

export function isPublicJevAddress(address) {
  if (typeof address !== 'string') return false;
  if (isIP(address) === 4) return ipv4GloballyRoutable(address);
  if (isIP(address) === 6) return ipv6GloballyRoutable(address);
  return false;
}
