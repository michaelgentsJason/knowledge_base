class MyConfig:

    def __init__(self, nacos_cfg):
        """根据naocs_cfg，调用load_confg，并将结构挂载到self上"""
        self.nacos_cfg = nacos_cfg

    async def load_config(self):
        """通过self.nacos_cfg和nacos的py sdk来获取"""


    def get_redis_config(self):
        """获取redis的配置"""
        return {"host": "120.232.79.83", "port": 26379, "db": 0, 'password':'redispass'}

    def get_milvus_config(self):
        """获取Milvus的配置"""
        return {
            "uri": "http://120.232.79.83:19530",
            "database": "test_db",
            # "collection": "sales_platform",
        }

    def get_model_config(self):
        """获取模型的配置"""
        return {
            "embedding": {
                "model": "BAAI/bge-m3",
                "base_url": "http://10.33.0.167:8100/v1",
                "api_key": "empty",
                "n_dim":1024
            },
            "rerank": None,
        }


my_config = MyConfig({})