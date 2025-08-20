"""初始化redis stack数据库链接"""

import redis.asyncio as redis


from src.service.config import my_config


class RedisClient:
    client: redis.Redis

    def __init__(self):
        self.cfg = my_config.get_redis_config()
        self.client = redis.Redis(
            host=self.cfg.get('host'),
            port=self.cfg.get('port'),
            db=self.cfg.get('db'),
            # password=self.cfg.get('password'),
            decode_responses=True
        )
        self.ping() # 如果ping出错，则证明初始化失败

    def ping(self):
        """测试连接"""
        return self.client.ping()

    def get_client(self) -> redis.Redis:
        """获取已经初始化的client"""
        return self.client


redis_client = RedisClient()

