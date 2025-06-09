
#!/usr/bin/env python3
"""
High-Performance Redis to Dragonfly Migration Script
Uses parallel processing, pipelining, and DUMP/RESTORE for maximum speed.
"""

import redis
import time
import logging
from typing import Dict, Any, List, Optional
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
import signal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FastRedisToMDragonflyMigrator:
    def __init__(self, 
                 source_config: Dict[str, Any], 
                 dest_config: Dict[str, Any],
                 workers: int = 10,
                 pipeline_size: int = 100,
                 batch_size: int = 5000):
        """
        Initialize high-performance migrator.
        
        Args:
            source_config: Source Redis connection parameters
            dest_config: Destination Dragonfly connection parameters
            workers: Number of parallel workers
            pipeline_size: Number of commands to pipeline
            batch_size: Keys per batch
        """
        # Connection pools for better performance
        self.source_pool = redis.ConnectionPool(**source_config, decode_responses=False, max_connections=workers*2)
        self.dest_pool = redis.ConnectionPool(**dest_config, decode_responses=False, max_connections=workers*2)
        
        self.workers = workers
        self.pipeline_size = pipeline_size
        self.batch_size = batch_size
        
        # Statistics
        self.keys_migrated = 0
        self.keys_failed = 0
        self.bytes_transferred = 0
        self.start_time = None
        self.stats_lock = threading.Lock()
        
        # For graceful shutdown
        self.shutdown = False
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully."""
        logger.warning("Received interrupt signal. Shutting down gracefully...")
        self.shutdown = True
        
    def _get_source_conn(self) -> redis.Redis:
        """Get a Redis connection from the pool."""
        return redis.Redis(connection_pool=self.source_pool)
    
    def _get_dest_conn(self) -> redis.Redis:
        """Get a Dragonfly connection from the pool."""
        return redis.Redis(connection_pool=self.dest_pool)
    
    def connect_test(self) -> bool:
        """Test connections to both instances."""
        try:
            conn = self._get_source_conn()
            conn.ping()
            logger.info("Successfully connected to source Redis instance")
        except Exception as e:
            logger.error(f"Failed to connect to source Redis: {e}")
            return False
            
        try:
            conn = self._get_dest_conn()
            conn.ping()
            logger.info("Successfully connected to destination Dragonfly instance")
        except Exception as e:
            logger.error(f"Failed to connect to destination Dragonfly: {e}")
            return False
            
        return True
    
    def _migrate_keys_dump_restore(self, keys: List[bytes]) -> tuple:
        """
        Migrate keys using DUMP/RESTORE for maximum performance.
        Returns (success_count, failed_count, bytes_transferred)
        """
        source = self._get_source_conn()
        dest = self._get_dest_conn()
        pipeline = dest.pipeline(transaction=False)
        
        success = 0
        failed = 0
        bytes_count = 0
        
        for i, key in enumerate(keys):
            if self.shutdown:
                break
                
            try:
                # Use DUMP/RESTORE for fastest migration
                serialized = source.dump(key)
                if serialized:
                    ttl = source.pttl(key)
                    ttl = 0 if ttl < 0 else ttl
                    
                    pipeline.restore(key, ttl, serialized, replace=True)
                    bytes_count += len(serialized)
                    
                    # Execute pipeline periodically
                    if (i + 1) % self.pipeline_size == 0:
                        pipeline.execute()
                        pipeline = dest.pipeline(transaction=False)
                    
                    success += 1
                else:
                    success += 1  # Key doesn't exist
                    
            except redis.ResponseError as e:
                # Fallback to type-specific migration if DUMP/RESTORE fails
                if "DUMP payload version or checksum are wrong" in str(e):
                    if self._migrate_key_fallback(source, dest, key):
                        success += 1
                    else:
                        failed += 1
                else:
                    failed += 1
                    
            except Exception as e:
                logger.debug(f"Error migrating key: {e}")
                failed += 1
        
        # Execute remaining commands in pipeline
        if pipeline.command_stack:
            pipeline.execute()
            
        return success, failed, bytes_count
    
    def _migrate_key_fallback(self, source: redis.Redis, dest: redis.Redis, key: bytes) -> bool:
        """Fallback migration method for when DUMP/RESTORE fails."""
        try:
            key_type = source.type(key)
            ttl = source.ttl(key)
            
            if key_type == b'string':
                value = source.get(key)
                dest.set(key, value)
            elif key_type == b'hash':
                data = source.hgetall(key)
                if data:
                    dest.hset(key, mapping=data)
            elif key_type == b'list':
                # Use LRANGE with pagination for large lists
                batch_size = 1000
                start = 0
                dest.delete(key)
                while True:
                    batch = source.lrange(key, start, start + batch_size - 1)
                    if not batch:
                        break
                    dest.rpush(key, *batch)
                    if len(batch) < batch_size:
                        break
                    start += batch_size
            elif key_type == b'set':
                # Use SSCAN for large sets
                cursor = 0
                dest.delete(key)
                while True:
                    cursor, members = source.sscan(key, cursor, count=1000)
                    if members:
                        dest.sadd(key, *members)
                    if cursor == 0:
                        break
            elif key_type == b'zset':
                # Use ZSCAN for large sorted sets
                cursor = 0
                dest.delete(key)
                while True:
                    cursor, data = source.zscan(key, cursor, count=1000)
                    if data:
                        # Convert to zadd format
                        mapping = {}
                        for i in range(0, len(data), 2):
                            mapping[data[i]] = data[i + 1]
                        dest.zadd(key, mapping)
                    if cursor == 0:
                        break
            else:
                return False
            
            if ttl > 0:
                dest.expire(key, ttl)
                
            return True
            
        except Exception as e:
            logger.debug(f"Fallback migration failed for key: {e}")
            return False
    
    def _worker(self, work_queue: Queue) -> None:
        """Worker thread to process batches of keys."""
        while not self.shutdown:
            try:
                keys = work_queue.get(timeout=1)
                if keys is None:  # Poison pill
                    break
                    
                success, failed, bytes_count = self._migrate_keys_dump_restore(keys)
                
                with self.stats_lock:
                    self.keys_migrated += success
                    self.keys_failed += failed
                    self.bytes_transferred += bytes_count
                    
                    # Log progress
                    total = self.keys_migrated + self.keys_failed
                    if total % 10000 == 0:
                        elapsed = time.time() - self.start_time
                        rate = total / elapsed if elapsed > 0 else 0
                        mb_transferred = self.bytes_transferred / (1024 * 1024)
                        logger.info(
                            f"Processed {total:,} keys ({rate:,.0f} keys/sec, "
                            f"{mb_transferred:.1f} MB transferred)"
                        )
                        
            except Exception as e:
                logger.error(f"Worker error: {e}")
                continue
    
    def migrate(self, pattern: str = '*', flush_dest: bool = False) -> None:
        """
        Perform high-performance migration.
        
        Args:
            pattern: Pattern to match keys (default: '*' for all keys)
            flush_dest: Whether to flush destination before migration
        """
        self.start_time = time.time()
        
        if not self.connect_test():
            logger.error("Connection test failed. Aborting migration.")
            return
        
        source = self._get_source_conn()
        dest = self._get_dest_conn()
        
        # Get source info
        try:
            db_size = source.dbsize()
            logger.info(f"Source database contains {db_size:,} keys")
        except:
            logger.info("Starting migration...")
        
        if flush_dest:
            logger.warning("Flushing destination database...")
            dest.flushdb()
            logger.info("Destination database flushed")
        
        # Create work queue and start workers
        work_queue = Queue(maxsize=self.workers * 2)
        workers = []
        
        logger.info(f"Starting {self.workers} worker threads...")
        for _ in range(self.workers):
            t = threading.Thread(target=self._worker, args=(work_queue,))
            t.start()
            workers.append(t)
        
        # Scan keys and distribute to workers
        logger.info(f"Scanning keys with pattern '{pattern}'...")
        cursor = 0
        batch = []
        scan_count = 10000  # Larger scan count for better performance
        
        try:
            while not self.shutdown:
                cursor, keys = source.scan(
                    cursor=cursor,
                    match=pattern,
                    count=scan_count
                )
                
                batch.extend(keys)
                
                # Send batch to workers
                if len(batch) >= self.batch_size:
                    work_queue.put(batch[:self.batch_size])
                    batch = batch[self.batch_size:]
                
                if cursor == 0:
                    break
            
            # Send remaining keys
            if batch and not self.shutdown:
                work_queue.put(batch)
                
        except Exception as e:
            logger.error(f"Error during key scanning: {e}")
        
        # Signal workers to stop
        for _ in range(self.workers):
            work_queue.put(None)
        
        # Wait for workers to finish
        logger.info("Waiting for workers to complete...")
        for t in workers:
            t.join()
        
        self._print_summary()
    
    def _print_summary(self) -> None:
        """Print migration summary."""
        elapsed = time.time() - self.start_time
        total = self.keys_migrated + self.keys_failed
        
        logger.info("=" * 60)
        logger.info("Migration completed!")
        logger.info(f"Total keys processed: {total:,}")
        logger.info(f"Successfully migrated: {self.keys_migrated:,}")
        logger.info(f"Failed: {self.keys_failed:,}")
        logger.info(f"Data transferred: {self.bytes_transferred / (1024**2):.1f} MB")
        logger.info(f"Time elapsed: {elapsed:.2f} seconds")
        if elapsed > 0:
            logger.info(f"Average rate: {total/elapsed:,.0f} keys/second")
            logger.info(f"Transfer rate: {self.bytes_transferred/elapsed/(1024**2):.1f} MB/second")
        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='High-performance Redis to Dragonfly migration'
    )
    
    # Source Redis arguments
    parser.add_argument('--source-host', required=True, help='Source Redis host')
    parser.add_argument('--source-port', type=int, default=6379, help='Source Redis port')
    parser.add_argument('--source-password', help='Source Redis password')
    parser.add_argument('--source-db', type=int, default=0, help='Source Redis database number')
    
    # Destination Dragonfly arguments
    parser.add_argument('--dest-host', required=True, help='Destination Dragonfly host')
    parser.add_argument('--dest-port', type=int, default=6379, help='Destination Dragonfly port')
    parser.add_argument('--dest-password', help='Destination Dragonfly password')
    parser.add_argument('--dest-db', type=int, default=0, help='Destination Dragonfly database number')
    
    # Performance options
    parser.add_argument('--workers', type=int, default=100, help='Number of parallel workers')
    parser.add_argument('--pipeline-size', type=int, default=100, help='Redis pipeline size')
    parser.add_argument('--batch-size', type=int, default=5000, help='Keys per batch')
    
    # Migration options
    parser.add_argument('--pattern', default='*', help='Key pattern to migrate (default: *)')
    parser.add_argument('--flush-dest', action='store_true', help='Flush destination before migration')
    
    args = parser.parse_args()
    
    # Build connection configs
    source_config = {
        'host': args.source_host,
        'port': args.source_port,
        'db': args.source_db,
    }
    if args.source_password:
        source_config['password'] = args.source_password
    
    dest_config = {
        'host': args.dest_host,
        'port': args.dest_port,
        'db': args.dest_db,
    }
    if args.dest_password:
        dest_config['password'] = args.dest_password
    
    # Create migrator and run
    migrator = FastRedisToMDragonflyMigrator(
        source_config=source_config,
        dest_config=dest_config,
        workers=args.workers,
        pipeline_size=args.pipeline_size,
        batch_size=args.batch_size
    )
    
    try:
        migrator.migrate(pattern=args.pattern, flush_dest=args.flush_dest)
    except KeyboardInterrupt:
        logger.warning("Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)



if __name__ == "__main__":
    main()