import socket
import concurrent.futures
from typing import Optional

def check_port_available(host: str, port: int, timeout: float = 0.5) -> bool:
    """
    Check if a specified port is available (not in use)

    Args:
        host: Target host address
        port: Port number to check
        timeout: Connection timeout in seconds

    Returns:
        True if port is available, False if port is occupied
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            # connect_ex returns 0 = connection succeeded (port occupied)
            # returns non-zero = connection failed (port likely free)
            result = sock.connect_ex((host, port))
            return result != 0
    except Exception:
        return False

def find_available_port(
    host: str = "host.docker.internal",
    start_port: int = 1024,
    end_port: int = 65535,
    max_workers: int = 100,
    timeout: float = 0.5
) -> Optional[int]:
    """
    Find the first available port in the specified range, exit immediately once found

    Args:
        host: Target host address
        start_port: Start port number (default 1024, skip well-known system ports)
        end_port: End port number
        max_workers: Maximum thread pool workers
        timeout: Timeout per port check in seconds

    Returns:
        First available port number, return None if no available port found
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_port = {
            executor.submit(check_port_available, host, port, timeout): port
            for port in range(start_port, end_port + 1)
        }

        for future in concurrent.futures.as_completed(future_to_port):
            port = future_to_port[future]
            try:
                if future.result():
                    executor.shutdown(cancel_futures=True)
                    return port
            except Exception as e:
                print(f"Error checking port {port}: {e}")

    return None
