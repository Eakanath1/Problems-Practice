"""
Token Bucket Rate Limiter - Threading Version (Extensible Design)

Architecture:
- RateLimiterStrategy: Abstract interface for all rate limiting strategies
- RateLimiterType: Enum for different algorithm types
- RateLimiterFactory: Factory pattern for creating rate limiters
- TokenBucketStrategy: Token bucket implementation
- RateLimiterController: Processes requests using any strategy
"""

import threading
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, Optional, Callable, List
from concurrent.futures import ThreadPoolExecutor, Future

# ========== STRATEGY INTERFACE ==========


class RateLimiterStrategy(ABC):
    """Interface for all rate limiting strategies."""

    @abstractmethod
    def give_access(self, rate_limit_key: Optional[str]) -> bool:
        """Check if request should be allowed."""
        pass

    @abstractmethod
    def update_configuration(self, config: Dict[str, Any]) -> None:
        """Update configuration dynamically."""
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Clean up resources."""
        pass


# ========== RATE LIMITER TYPES ==========


class RateLimiterType(Enum):
    """Supported rate limiting algorithms."""

    TOKEN_BUCKET = "token_bucket"
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"


# ========== TOKEN BUCKET STRATEGY ==========


class TokenBucketStrategy(RateLimiterStrategy):
    """Token Bucket algorithm implementation with per-key locking."""

    class Bucket:
        """Inner class representing an individual token bucket."""

        def __init__(self, initial_tokens: int):
            self.tokens = initial_tokens

        def try_consume(self) -> bool:
            """Attempt to consume one token (caller must hold lock)."""
            if self.tokens > 0:
                self.tokens -= 1
                return True
            return False

        def refill(self, capacity: int, refill_rate: int):
            """Refill tokens up to capacity (caller must hold lock)."""
            self.tokens = min(capacity, self.tokens + refill_rate)

    def __init__(self, bucket_capacity: int, refresh_rate: int):
        """
        Args:
            bucket_capacity: Maximum tokens per bucket
            refresh_rate: Tokens added per second
        """
        self.bucket_capacity = bucket_capacity
        self.refresh_rate = refresh_rate
        self.global_bucket = self.Bucket(bucket_capacity)
        self.global_lock = threading.Lock()
        # Per-key locking: each user key has its own lock and bucket
        self.user_buckets: Dict[str, "TokenBucketStrategy.Bucket"] = {}
        self.user_locks: Dict[str, threading.Lock] = {}
        self.dict_lock = threading.Lock()  # Lock for dictionary modifications
        self.refill_thread = None
        self.running = False

    def start_refill_task(self):
        """Start the scheduled refill thread."""
        self.running = True
        self.refill_thread = threading.Thread(target=self._refill_loop, daemon=True)
        self.refill_thread.start()

    def _refill_loop(self):
        """Background thread to refill all buckets."""
        while self.running:
            time.sleep(1)  # 1 second interval

            # Refill global bucket
            with self.global_lock:
                self.global_bucket.refill(self.bucket_capacity, self.refresh_rate)

            # Refill user buckets - get snapshot of keys to avoid dict modification during iteration
            with self.dict_lock:
                user_keys = list(self.user_locks.keys())

            for key in user_keys:
                # Acquire per-key lock for safe refill
                with self.dict_lock:
                    if key not in self.user_locks:
                        continue  # Key was removed
                    lock = self.user_locks[key]

                with lock:
                    if key in self.user_buckets:
                        self.user_buckets[key].refill(
                            self.bucket_capacity, self.refresh_rate
                        )

    def give_access(self, rate_limit_key: Optional[str]) -> bool:
        """Check if request is allowed based on token availability."""
        if rate_limit_key:
            # Get or create per-key lock
            with self.dict_lock:
                if rate_limit_key not in self.user_locks:
                    self.user_locks[rate_limit_key] = threading.Lock()
                    self.user_buckets[rate_limit_key] = self.Bucket(
                        self.bucket_capacity
                    )
                lock = self.user_locks[rate_limit_key]

            # Acquire per-key lock and consume token atomically
            with lock:
                bucket = self.user_buckets[rate_limit_key]
                return bucket.try_consume()
        else:
            # Global bucket
            with self.global_lock:
                return self.global_bucket.try_consume()

    def update_configuration(self, config: Dict[str, Any]) -> None:
        """Update the refresh rate dynamically."""
        if "refresh_rate" in config:
            self.refresh_rate = config["refresh_rate"]

    def shutdown(self) -> None:
        """Stop the refill thread."""
        self.running = False
        if self.refill_thread:
            self.refill_thread.join(timeout=2)


# ========== FACTORY PATTERN ==========


class RateLimiterFactory:
    """Factory for creating rate limiter instances."""

    _factories: Dict[
        RateLimiterType, Callable[[Dict[str, Any]], RateLimiterStrategy]
    ] = {}

    @classmethod
    def register_factory(
        cls,
        limiter_type: RateLimiterType,
        factory: Callable[[Dict[str, Any]], RateLimiterStrategy],
    ):
        """Register a new rate limiter factory."""
        cls._factories[limiter_type] = factory

    @classmethod
    def create_limiter(
        cls, limiter_type: RateLimiterType, config: Dict[str, Any]
    ) -> RateLimiterStrategy:
        """Create a rate limiter of the specified type."""
        factory = cls._factories.get(limiter_type)
        if not factory:
            raise ValueError(f"Unsupported rate limiter type: {limiter_type}")
        return factory(config)


# Register Token Bucket factory
def _create_token_bucket(config: Dict[str, Any]) -> TokenBucketStrategy:
    capacity = config.get("capacity", 10)
    refresh_rate = config.get("refresh_rate", 1)
    strategy = TokenBucketStrategy(capacity, refresh_rate)
    strategy.start_refill_task()
    return strategy


RateLimiterFactory.register_factory(RateLimiterType.TOKEN_BUCKET, _create_token_bucket)


# ========== CONTROLLER ==========


class RateLimiterController:
    """Controller that processes requests using a rate limiting strategy."""

    def __init__(self, limiter_type: RateLimiterType, config: Dict[str, Any]):
        """
        Args:
            limiter_type: Type of rate limiter to use
            config: Configuration for the rate limiter
        """
        self.rate_limiter = RateLimiterFactory.create_limiter(limiter_type, config)
        self.executor = ThreadPoolExecutor(max_workers=10)

    def process_request(self, rate_limit_key: Optional[str] = None) -> bool:
        """
        Process a request with rate limiting.

        Args:
            rate_limit_key: Key for per-user limiting, None for global

        Returns:
            True if allowed, False if blocked
        """
        allowed = self.rate_limiter.give_access(rate_limit_key)
        status = "✅ Allowed" if allowed else "❌ Blocked"
        print(f"Request [{rate_limit_key or 'global'}]: {status}")
        return allowed

    def update_configuration(self, config: Dict[str, Any]):
        """Update rate limiter configuration."""
        self.rate_limiter.update_configuration(config)

    def shutdown(self):
        """Shutdown the rate limiter and executor."""
        self.rate_limiter.shutdown()
        self.executor.shutdown(wait=True)


# ========== CLIENT CODE ==========


def demo_burst_requests(
    controller: RateLimiterController, count: int, user_key: Optional[str] = None
):
    """Send a burst of requests concurrently."""
    futures: List[Future] = []
    for _ in range(count):
        future = controller.executor.submit(controller.process_request, user_key)
        futures.append(future)

    # Wait for all to complete
    results = [f.result() for f in futures]
    allowed = sum(results)
    blocked = count - allowed
    print(f"Results: {allowed} allowed, {blocked} blocked (total: {count})\n")


def main():
    """Demonstrate the rate limiter with extensible design."""
    # Create controller using factory pattern
    config = {"capacity": 5, "refresh_rate": 1}
    controller = RateLimiterController(RateLimiterType.TOKEN_BUCKET, config)

    # Example 1: Global rate limiting - burst
    print("=== EXAMPLE 1: Global rate limiting - Burst ===")
    demo_burst_requests(controller, 10)

    # Example 2: Wait for refill
    print("=== EXAMPLE 2: After 3 seconds refill ===")
    print("Waiting 3.2 seconds...")
    time.sleep(3.2)  # Small buffer to ensure all refills complete
    demo_burst_requests(controller, 10)

    # Example 3: Per-user rate limiting
    print("=== EXAMPLE 3: Per-user rate limiting ===")
    for user in ["user1", "user2", "user3"]:
        print(f"Requests for {user}:")
        demo_burst_requests(controller, 7, user)

    # Example 4: High concurrency
    print("=== EXAMPLE 4: High concurrency ===")
    futures = [
        controller.executor.submit(controller.process_request) for _ in range(20)
    ]
    results = [f.result() for f in futures]
    allowed = sum(results)
    print(f"High concurrency: {allowed} allowed, {20 - allowed} blocked\n")

    controller.shutdown()


if __name__ == "__main__":
    main()
