"""
redis stack的增删改查逻辑
"""
import redis.asyncio as redis

from src.utils.logger import logger
from src.dbs.redis_stack.init import redis_client
from src.service.config import my_config

cfg = my_config.get_redis_config()
# 获取 client 对象
client = redis_client.get_client()

# 使用 assert 来告诉 IDE 和运行时 client 的确切类型,以便于直接使用client内部的方法
assert isinstance(client, redis.Redis)

# 辅助函数

def parse_search_result(result):
    """辅助函数 解析redis的搜索结果"""
    if not result or len(result) < 2:
        return []

    # total_results = result[0]
    parsed_results = []

    # 结果格式：[total, key1, [field1, value1, field2, value2], key2, [...]]
    for i in range(1, len(result), 2):
        if i + 1 < len(result):
            key = result[i]
            fields = result[i + 1]

            # 解析字段值对
            result_dict = {'key': key}
            for j in range(0, len(fields), 2):
                if j + 1 < len(fields):
                    field_name = fields[j]
                    field_value = fields[j + 1]
                    result_dict[field_name] = field_value

            parsed_results.append(result_dict)

    return parsed_results


# 功能函数

async def create_hotspot_index(group_id):
    """创建热点问题向量索引，只在每个公司第一次创建热点问题时使用, 每个公司一个group_id"""
    try:
        # 先尝试删除旧索引（如果存在）
        try:
            await client.execute_command('FT.DROPINDEX', group_id)
            print(f"🗑️ 删除旧索引: {group_id}")
        except Exception:
            logger.warning(f'热点问题分组:{group_id}已存在，无需重新创建！')

        # 创建新的1024维索引
        await client.execute_command(
            'FT.CREATE', group_id,
            'ON', 'JSON',
            'PREFIX', '1', group_id,
            'SCHEMA',
            '$.query_vector', 'AS', 'vector', 'VECTOR', 'FLAT', '6',
            'TYPE', 'FLOAT32', 'DIM', str(cfg.get('n_dim')),  # 现在是1024
            'DISTANCE_METRIC', 'COSINE',
            '$.category', 'AS', 'category', 'TAG',
            '$.question', 'AS', 'question', 'TEXT',
            '$.question_id', 'AS', 'question_id', 'TEXT'
        )
        logger.info(f"✅ 索引 {group_id} 创建成功 (维度: {str(cfg.get('n_dim'))})")
        return True
    except Exception as e:
        logger.error(f"ℹ️ 索引 {group_id} 创建失败，错误原因:{e}")
        return False

async def store_hotspot_question( question_id: str, data: dict, group_id:str):
    """存储热点问题"""
    key = f"{group_id}{question_id}"

    try:
        required_keys = {"query_vector", "category", "question", 'question_id'}
        if not isinstance(data, dict) or not required_keys.issubset(data.keys()):
            error_msg = f"❌ 存储失败: 传入的数据结构不符合索引要求。需要包含的键: {required_keys}"
            logger.info(error_msg)
            # 或者可以 raise 一个更具体的异常
            # raise ValueError(error_msg)
            return False

        await client.json().set(key, '$', data) # 这里实际返回的也是一个异步
        return True
    except Exception as e:
        print(f"❌ 存储失败: {e}")
        return False


async def get_hotspot_question(group_id:str, question_id: str):
    """获取热点问题"""
    key = f"{group_id}{question_id}"
    try:
        return await client.json().get(key)
    except Exception as e:
        print(f"❌ 获取失败: {e}")
        return None



async def list_all_questions( group_id: str = None, limit: int = 10):
    """获取固定分组的所有热点问题"""
    try:
        # 构建搜索条件
        if not group_id:
            return []
        query = "*"

        result = await client.execute_command(
            'FT.SEARCH', group_id, query,
            'LIMIT', '0', str(limit),
            # 'RETURN', '5', 'question', 'standard_reply', 'category', 'hit_count', 'created_at' # TODO :命中计数这个逻辑后续要放到后端实现
            'RETURN', '5', 'question', 'standard_reply', 'category', 'question_id'
        )

        return parse_search_result(result)
    except Exception as e:
        print(f"❌ 获取问题列表失败: {e}")
        return []


async def delete_hotspot_question(group_id:str, question_id: str):
    """删除热点问题"""
    key = f"{group_id}{question_id}"
    try:

        result = await client.delete(key)
        return result > 0
    except Exception as e:
        print(f"❌ 删除失败: {e}")
        return False


async def delete_questions_by_category(group_id:str, category: str):
    """按分类删除问题"""
    try:
        # 先搜索该分类的所有问题
        result = await client.execute_command(
            'FT.SEARCH', group_id, f"@category:{category}",
            'RETURN', '0'  # 只返回key
        )

        if len(result) < 2:
            return 0

        # 删除找到的keys
        count = 0
        for i in range(1, len(result), 2):
            key = result[i]
            if client.delete(key):
                count += 1

        return count
    except Exception as e:
        print(f"❌ 按分类删除失败: {e}")
        return 0


async def get_stats(group_id:str):
    """获取统计信息"""
    try:
        # 获取索引信息
        index_info = await client.execute_command('FT.INFO', group_id)

        # 获取所有热点问题的key
        keys = await client.keys(f"{group_id}*")

        # 统计分类
        categories = {}
        # total_hits = 0

        for key in keys:
            try:
                data = await client.json().get(key)
                if data:
                    cat = data.get('category', '未分类')
                    categories[cat] = categories.get(cat, 0) + 1
                    # total_hits += data.get('hit_count', 0)
            except:
                continue

        return {
            "total_questions": len(keys),
            # "total_hits": total_hits,
            "categories": categories,
            "index_status": "active" if index_info else "error"
        }
    except Exception as e:
        print(f"❌ 获取统计失败: {e}")
        return {"error": str(e)}