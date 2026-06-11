import socket
import urllib.request
import json
import logging

logger = logging.getLogger("MLF.DNSPatch")

# Cache for resolved domains to avoid repeated DoH calls
dns_cache = {}

original_getaddrinfo = socket.getaddrinfo

def doh_resolve(host):
    # If it's an IP address, return it as-is
    try:
        socket.inet_aton(host)
        return [host]
    except socket.error:
        pass

    if host in dns_cache:
        return dns_cache[host]

    # Only resolve Binance domains using DoH to avoid affecting local/internal name resolution
    if "binance" in host:
        # Use direct IP addresses for Cloudflare and Google DoH to bypass local DNS resolution entirely.
        urls = [
            f"https://1.1.1.1/dns-query?name={host}",
            f"https://8.8.8.8/resolve?name={host}"
        ]
        
        for url in urls:
            try:
                req = urllib.request.Request(url, headers={'accept': 'application/dns-json'})
                with urllib.request.urlopen(req, timeout=3) as response:
                    data = json.loads(response.read().decode())
                    ips = []
                    if "Answer" in data:
                        for answer in data["Answer"]:
                            # Type 1 is A record
                            if answer.get("type") == 1:
                                ips.append(answer["data"])
                    if ips:
                        dns_cache[host] = ips
                        logger.info(f"Resolved {host} via DoH: {ips}")
                        return ips
            except Exception as e:
                logger.warning(f"Failed to resolve {host} via DoH ({url}): {e}")
                
    return []

def custom_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    ips = doh_resolve(host)
    if ips:
        results = []
        for ip in ips:
            try:
                res = original_getaddrinfo(ip, port, family, type, proto, flags)
                for r in res:
                    results.append(r)
            except Exception:
                pass
        if results:
            return results

    return original_getaddrinfo(host, port, family, type, proto, flags)

def apply_patch():
    """Applies the custom getaddrinfo DNS patch."""
    if socket.getaddrinfo != custom_getaddrinfo:
        logger.info("Applying DNS patch (monkey-patching socket.getaddrinfo)...")
        socket.getaddrinfo = custom_getaddrinfo
