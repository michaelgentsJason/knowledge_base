"""
redis stack的增删改查逻辑 - 进一步优化版
添加了批量操作、缓存、连接池等优化
"""
import redis.asyncio as redis
import asyncio
from typing import List, Dict, Any, Optional
import json
from datetime import datetime, timedelta

from src.utils.logger import logger
from src.dbs.redis_stack.init import redis_client
from src.service.config import my_config

cfg = my_config.get_redis_config()
client = redis_client.get_client()
assert isinstance(client, redis.Redis)

# 缓存配置
CACHE_TTL = 300  # 缓存5分钟
STATS_CACHE_KEY = "hotspot:stats:{group_id}"
INDEX_STATUS_CACHE = "hotspot:index_status"

# 辅助函数

def parse_search_result(result):
    """辅助函数 解析redis的搜索结果"""
    if not result or len(result) < 2:
        return []

    parsed_results = []
    for i in range(1, len(result), 2):
        if i + 1 < len(result):
            key = result[i]
            fields = result[i + 1]

            result_dict = {'key': key}
            for j in range(0, len(fields), 2):
                if j + 1 < len(fields):
                    field_name = fields[j]
                    field_value = fields[j + 1]
                    result_dict[field_name] = field_value

            parsed_results.append(result_dict)

    return parsed_results


def parse_vector_search_result(result):
    """解析向量搜索结果，包含相似度评分"""
    if not result or len(result) < 2:
        return []

    parsed_results = []

    for i in range(1, len(result), 2):
        if i + 1 < len(result):
            key = result[i]
            fields = result[i + 1]

            result_dict = {'key': key}
            similarity_score = None

            for j in range(0, len(fields), 2):
                if j + 1 < len(fields):
                    field_name = fields[j]
                    field_value = fields[j + 1]

                    if field_name == '__vector_score':
                        # 转换为相似度（1 - 距离）
                        similarity_score = 1.0 - float(field_value)
                    else:
                        result_dict[field_name] = field_value

            if similarity_score is not None:
                result_dict['similarity_score'] = round(similarity_score, 4)

            parsed_results.append(result_dict)

    return parsed_results


# 索引管理优化

async def create_hotspot_index(group_id: str, force_recreate: bool = False):
    """
    创建热点问题向量索引
    :param group_id: 分组ID
    :param force_recreate: 是否强制重建索引
    """
    try:
        # 检查索引状态缓存
        if not force_recreate:
            cached_status = await client.get(f"{INDEX_STATUS_CACHE}:{group_id}")
            if cached_status == "active":
                logger.info(f"索引 {group_id} 已存在且正常，跳过创建")
                return True

        # 尝试删除旧索引
        try:
            await client.execute_command('FT.DROPINDEX', group_id)
            logger.info(f"🗑️ 删除旧索引: {group_id}")
        except Exception:
            logger.info(f"索引 {group_id} 不存在，准备创建新索引")

        # 创建新索引
        await client.execute_command(
            'FT.CREATE', group_id,
            'ON', 'JSON',
            'PREFIX', '1', group_id,
            'SCHEMA',
            '$.query_vector', 'AS', 'vector', 'VECTOR', 'FLAT', '6',
            'TYPE', 'FLOAT32', 'DIM', str(cfg.get('n_dim', 1024)),
            'DISTANCE_METRIC', 'COSINE',
            '$.category', 'AS', 'category', 'TAG',
            '$.question', 'AS', 'question', 'TEXT',
            '$.question_id', 'AS', 'question_id', 'TEXT',
            '$.created_at', 'AS', 'created_at', 'TEXT',
            '$.updated_at', 'AS', 'updated_at', 'TEXT'
        )

        # 缓存索引状态
        await client.setex(f"{INDEX_STATUS_CACHE}:{group_id}", CACHE_TTL, "active")

        logger.info(f"✅ 索引 {group_id} 创建成功 (维度: {cfg.get('n_dim', 1024)})")
        return True

    except Exception as e:
        logger.error(f"❌ 索引 {group_id} 创建失败: {e}")
        return False


async def check_index_exists(group_id: str) -> bool:
    """检查索引是否存在"""
    try:
        await client.execute_command('FT.INFO', group_id)
        return True
    except Exception:
        return False


# 批量操作优化

async def store_hotspot_questions_batch(questions_data: List[Dict], group_id: str):
    """
    批量存储热点问题 - 使用pipeline优化
    :param questions_data: 问题数据列表
    :param group_id: 分组ID
    :return: (成功数量, 失败数量, 失败详情)
    """
    if not questions_data:
        return 0, 0, []

    success_count = 0
    failed_items = []

    try:
        # 使用pipeline批量操作
        pipe = client.pipeline()

        # 验证数据格式
        required_keys = {"query_vector", "category", "question", "question_id"}
        valid_items = []

        for i, data in enumerate(questions_data):
            if not isinstance(data, dict) or not required_keys.issubset(data.keys()):
                failed_items.append({
                    "index": i,
                    "question_id": data.get("question_id", f"unknown_{i}"),
                    "reason": "数据格式不符合要求"
                })
                continue
            valid_items.append((i, data))

        # 批量设置JSON数据
        for original_index, data in valid_items:
            key = f"{group_id}{data['question_id']}"
            pipe.json().set(key, '$', data)

        # 执行批量操作
        if valid_items:
            results = await pipe.execute()

            # 处理结果
            for (original_index, data), result in zip(valid_items, results):
                if result:  # Redis JSON.SET 成功返回 'OK'
                    success_count += 1
                else:
                    failed_items.append({
                        "index": original_index,
                        "question_id": data["question_id"],
                        "reason": "Redis存储失败"
                    })

        logger.info(f"批量存储完成: 成功 {success_count}, 失败 {len(failed_items)}")
        return success_count, len(failed_items), failed_items

    except Exception as e:
        logger.error(f"批量存储异常: {e}")
        # 如果批量操作失败，尝试逐个存储
        return await _fallback_store_individual(questions_data, group_id)


async def _fallback_store_individual(questions_data: List[Dict], group_id: str):
    """批量操作失败时的回退方案：逐个存储"""
    logger.warning("批量操作失败，回退到逐个存储模式")

    success_count = 0
    failed_items = []

    for i, data in enumerate(questions_data):
        try:
            success = await store_hotspot_question(
                question_id=data["question_id"],
                data=data,
                group_id=group_id
            )
            if success:
                success_count += 1
            else:
                failed_items.append({
                    "index": i,
                    "question_id": data["question_id"],
                    "reason": "单个存储失败"
                })
        except Exception as e:
            failed_items.append({
                "index": i,
                "question_id": data.get("question_id", f"unknown_{i}"),
                "reason": str(e)
            })

    return success_count, len(failed_items), failed_items


async def store_hotspot_question(question_id: str, data: dict, group_id: str):
    """存储单个热点问题"""
    key = f"{group_id}{question_id}"

    try:
        required_keys = {"query_vector", "category", "question", "question_id"}
        if not isinstance(data, dict) or not required_keys.issubset(data.keys()):
            logger.warning(f"数据格式不符合要求: {question_id}")
            return False

        await client.json().set(key, '$', data)
        logger.debug(f"✅ 存储问题: {question_id}")
        return True

    except Exception as e:
        logger.error(f"❌ 存储失败 {question_id}: {e}")
        return False


# 搜索功能优化

async def vector_search_questions(
    group_id: str,
    query_vector: List[float],
    limit: int = 3,
    category: str = None,
    min_similarity: float = 0.0
):
    """
    优化的向量搜索
    :param group_id: 分组ID
    :param query_vector: 查询向量
    :param limit: 返回数量限制
    :param category: 可选的分类过滤
    :param min_similarity: 最小相似度阈值
    """
    try:
        # 检查索引是否存在
        if not await check_index_exists(group_id):
            logger.warning(f"索引 {group_id} 不存在，尝试创建")
            await create_hotspot_index(group_id)

        # 构建向量字符串
        vector_blob = ','.join(map(str, query_vector))

        # 构建查询条件
        if category:
            query = f"@category:{{{category}}}=>[KNN {limit} @vector $query_vector AS __vector_score]"
        else:
            query = f"*=>[KNN {limit} @vector $query_vector AS __vector_score]"

        logger.debug(f"执行向量搜索: group_id={group_id}, limit={limit}, category={category}")

        result = await client.execute_command(
            'FT.SEARCH', group_id, query,
            'PARAMS', '2', 'query_vector', vector_blob,
            'SORTBY', '__vector_score',
            'RETURN', '8', 'question_id', 'question', 'standard_reply', 'category',
            'related_links', 'created_at', 'updated_at', '__vector_score',
            'DIALECT', '2'
        )

        parsed_results = parse_vector_search_result(result)

        # 应用相似度过滤
        if min_similarity > 0:
            filtered_results = [
                r for r in parsed_results
                if r.get('similarity_score', 0) >= min_similarity
            ]
            logger.debug(f"相似度过滤: {len(parsed_results)} -> {len(filtered_results)}")
            parsed_results = filtered_results

        logger.info(f"向量搜索完成: 返回 {len(parsed_results)} 个结果")
        return parsed_results

    except Exception as e:
        logger.error(f"❌ 向量搜索失败: {e}")
        return []


# 缓存优化的统计功能

async def get_stats(group_id: str, use_cache: bool = True):
    """
    获取统计信息 - 支持缓存
    :param group_id: 分组ID
    :param use_cache: 是否使用缓存
    """
    cache_key = STATS_CACHE_KEY.format(group_id=group_id)

    try:
        # 尝试从缓存获取
        if use_cache:
            cached_stats = await client.get(cache_key)
            if cached_stats:
                try:
                    stats = json.loads(cached_stats)
                    logger.debug(f"使用缓存的统计信息: {group_id}")
                    return stats
                except json.JSONDecodeError:
                    logger.warning(f"缓存数据解析失败: {group_id}")

        # 重新计算统计信息
        stats = await _calculate_stats(group_id)

        # 缓存结果
        if use_cache and stats:
            await client.setex(cache_key, CACHE_TTL, json.dumps(stats))
            logger.debug(f"缓存统计信息: {group_id}")

        return stats

    except Exception as e:
        logger.error(f"❌ 获取统计信息失败: {e}")
        return {"error": str(e)}


async def _calculate_stats(group_id: str):
    """计算统计信息"""
    try:
        # 检查索引状态
        try:
            index_info = await client.execute_command('FT.INFO', group_id)
            index_status = "active"
        except:
            index_status = "not_found"

        # 获取所有问题的key
        keys = await client.keys(f"{group_id}*")

        # 统计分类和其他信息
        categories = {}
        total_size = 0
        latest_update = None

        # 使用pipeline批量获取数据
        if keys:
            pipe = client.pipeline()
            for key in keys:
                pipe.json().get(key, '$.category', '$.updated_at')

            results = await pipe.execute()

            for result in results:
                if result:
                    try:
                        category = result.get('$.category', ['未分类'])[0] if result.get('$.category') else '未分类'
                        categories[category] = categories.get(category, 0) + 1

                        # 跟踪最新更新时间
                        updated_at = result.get('$.updated_at', [None])[0] if result.get('$.updated_at') else None
                        if updated_at and (not latest_update or updated_at > latest_update):
                            latest_update = updated_at

                    except (KeyError, IndexError, TypeError):
                        categories['未分类'] = categories.get('未分类', 0) + 1

        stats = {
            "total_questions": len(keys),
            "categories": categories,
            "index_status": index_status,
            "last_updated": latest_update,
            "cache_time": datetime.now().isoformat()
        }

        logger.debug(f"计算统计信息完成: {group_id}")
        return stats

    except Exception as e:
        logger.error(f"计算统计信息失败: {e}")
        return {"error": str(e)}


# 清理和维护功能

async def cleanup_expired_cache():
    """清理过期的缓存"""
    try:
        # 获取所有缓存键
        cache_keys = await client.keys("hotspot:*")
        cleaned = 0

        for key in cache_keys:
            ttl = await client.ttl(key)
            if ttl == -1:  # 没有过期时间设置
                await client.expire(key, CACHE_TTL)
                cleaned += 1

        logger.info(f"缓存清理完成: 处理了 {cleaned} 个键")
        return cleaned

    except Exception as e:
        logger.error(f"缓存清理失败: {e}")
        return 0


async def get_hotspot_question(group_id: str, question_id: str):
    """获取热点问题"""
    key = f"{group_id}{question_id}"
    try:
        result = await client.json().get(key)
        if result:
            logger.debug(f"✅ 获取问题: {question_id}")
        else:
            logger.debug(f"⚠️ 问题不存在: {question_id}")
        return result
    except Exception as e:
        logger.error(f"❌ 获取失败: {e}")
        return None


async def list_all_questions(group_id: str = None, limit: int = 10, offset: int = 0):
    """获取问题列表 - 支持分页"""
    try:
        if not group_id:
            return []

        query = "*"

        result = await client.execute_command(
            'FT.SEARCH', group_id, query,
            'LIMIT', str(offset), str(limit),
            'RETURN', '7', 'question', 'standard_reply', 'category',
            'question_id', 'related_links', 'created_at', 'updated_at'
        )

        parsed_results = parse_search_result(result)
        logger.debug(f"获取问题列表: {len(parsed_results)} 个问题")
        return parsed_results

    except Exception as e:
        logger.error(f"❌ 获取问题列表失败: {e}")
        return []


async def delete_hotspot_question(group_id: str, question_id: str):
    """删除热点问题"""
    key = f"{group_id}{question_id}"
    try:
        result = await client.delete(key)
        if result > 0:
            # 清除相关缓存
            cache_key = STATS_CACHE_KEY.format(group_id=group_id)
            await client.delete(cache_key)
            logger.info(f"✅ 删除问题: {question_id}")
        else:
            logger.warning(f"⚠️ 问题不存在: {question_id}")
        return result > 0
    except Exception as e:
        logger.error(f"❌ 删除失败: {e}")
        return False


async def delete_questions_by_category(group_id: str, category: str):
    """按分类删除问题"""
    try:
        result = await client.execute_command(
            'FT.SEARCH', group_id, f"@category:{{{category}}}",
            'RETURN', '0'
        )

        if len(result) < 2:
            logger.info(f"分类 {category} 下没有问题需要删除")
            return 0

        # 使用pipeline批量删除
        pipe = client.pipeline()
        keys_to_delete = []

        for i in range(1, len(result), 2):
            key = result[i]
            keys_to_delete.append(key)
            pipe.delete(key)

        if keys_to_delete:
            results = await pipe.execute()
            count = sum(1 for r in results if r)

            # 清除缓存
            cache_key = STATS_CACHE_KEY.format(group_id=group_id)
            await client.delete(cache_key)

            logger.info(f"✅ 删除分类 {category}: {count} 个问题")
            return count

        return 0

    except Exception as e:
        logger.error(f"❌ 按分类删除失败: {e}")
        return 0