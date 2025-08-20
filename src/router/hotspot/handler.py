"""
热点问题处理器 - 业务逻辑实现
"""
import json
import uuid
import requests
from datetime import datetime
from typing import List, Optional, Dict

from src.utils.logger import logger
from src.dbs.redis_stack import curd
from src.router.hotspot.models import (
    QuestionInfo, AddQuestionRequest, AddQuestionBatchRequest,
    UpdateQuestionRequest, QueryRequest, ApiResponse
)
from src.service.config import my_config


class EmbeddingService:
    """向量化服务 (使用本地vLLM部署的bge-m3模型)"""

    def __init__(self, config: Dict[str, str] = None):
        # 新的配置：本地vLLM服务的地址和模型ID
        self.base_url = "http://127.0.0.1:8100/v1"  # vLLM OpenAI兼容接口的默认地址
        self.model_id = "BAAI/bge-m3"  # 您通过 "show available models" 看到的模型ID
        self.headers = {
            # 本地服务通常不需要API Key，所以认证头可以移除或简化
            "Content-Type": "application/json"
        }
        logger.info(f"EmbeddingService已切换至本地vLLM服务，模型: {self.model_id}")

    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        批量获取向量表示。
        vLLM的embedding接口本身就支持批量处理，效率更高。
        """
        # 过滤掉空字符串，避免不必要的API调用
        # 并记录原始索引，以便将结果正确地放回
        original_indices = [i for i, text in enumerate(texts) if text and text.strip()]
        texts_to_embed = [texts[i].strip() for i in original_indices]

        if not texts_to_embed:
            logger.warning("所有文本均为空，返回零向量列表")
            return [[0.0] * 1024 for _ in range(len(texts))]  # bge-m3的维度是1024

        url = f"{self.base_url}/embeddings"
        # 使用与OpenAI完全兼容的请求体
        payload = {
            "model": self.model_id,
            "input": texts_to_embed
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=60)  # 批量处理，超时时间可以适当延长
            response.raise_for_status()
            result = response.json()

            # 创建一个正确大小的零向量列表
            final_embeddings = [[0.0] * 1024 for _ in range(len(texts))]

            # 将获取到的向量根据原始索引放回正确的位置
            for i, data in enumerate(result["data"]):
                original_index = original_indices[i]
                final_embeddings[original_index] = data["embedding"]

            logger.info(f"成功从本地vLLM获取 {len(texts_to_embed)} 个向量")
            return final_embeddings

        except Exception as e:
            logger.error(f"调用本地vLLM向量化失败: {e}")
            # 如果请求失败，则所有文本都返回零向量
            return [[0.0] * 1024 for _ in range(len(texts))]

    def get_embedding(self, text: str) -> List[float]:
        """
        获取单个文本的向量表示（兼容性方法）
        内部调用批量接口
        """
        if not (text and text.strip()):
            logger.warning("输入文本为空，返回零向量")
            return [0.0] * 1024

        batch_result = self.get_embeddings_batch([text])
        return batch_result[0]


class HotspotHandler:
    """热点问题业务处理器"""

    def __init__(self):
        self.model_config = my_config.get_model_config()
        self.embedding_service = EmbeddingService()

    async def _get_text_embedding(self, text: str) -> List[float]:
        """
        获取文本的向量嵌入
        """
        try:
            # 调用本地vLLM embedding服务
            embedding = self.embedding_service.get_embedding(text)
            logger.debug(f"成功获取文本向量，维度: {len(embedding)}")
            return embedding
        except Exception as e:
            logger.error(f"获取文本向量失败: {str(e)}")
            # 返回零向量作为fallback
            return [0.0] * 1024

    async def add_question(self, request: AddQuestionRequest) -> ApiResponse:
        """添加单个热点问题"""
        try:
            # 1. 检查索引是否存在，不存在则创建
            index_created = await curd.create_hotspot_index(request.group_id)
            if not index_created:
                logger.warning(f"索引创建可能失败，但继续执行: {request.group_id}")

            # 2. 检查问题ID是否已存在
            existing = await curd.get_hotspot_question(request.group_id, request.question_info.question_id)
            if existing:
                return ApiResponse(
                    code=400,
                    status="error",
                    message=f"问题ID {request.question_info.question_id} 已存在"
                )

            # 3. 生成问题文本的向量嵌入
            question_vector = await self._get_text_embedding(request.question_info.question)

            # 4. 构建存储数据
            current_time = datetime.now().isoformat()
            store_data = {
                "question_id": request.question_info.question_id,
                "question": request.question_info.question,
                "standard_reply": request.question_info.standard_reply,
                "related_links": request.question_info.related_links or [],
                "category": request.question_info.category,
                "query_vector": question_vector,
                "created_at": current_time,
                "updated_at": current_time
            }

            # 5. 存储到Redis
            success = await curd.store_hotspot_question(
                question_id=request.question_info.question_id,
                data=store_data,
                group_id=request.group_id
            )

            if success:
                logger.info(f"成功添加热点问题: {request.question_info.question_id}")
                return ApiResponse(
                    code=200,
                    status="success",
                    message="问题添加成功",
                    data={"question_id": request.question_info.question_id}
                )
            else:
                return ApiResponse(
                    code=500,
                    status="error",
                    message="问题存储失败"
                )

        except Exception as e:
            logger.error(f"添加问题失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"添加问题失败: {str(e)}"
            )

    async def add_questions_batch(self, request: AddQuestionBatchRequest) -> ApiResponse:
        """批量添加热点问题 - 使用batch embedding提高效率"""
        try:
            # 1. 确保索引存在
            await curd.create_hotspot_index(request.group_id)

            # 2. 检查是否有重复的问题ID
            existing_questions = []
            for question_info in request.question_info_list:
                existing = await curd.get_hotspot_question(request.group_id, question_info.question_id)
                if existing:
                    existing_questions.append(question_info.question_id)

            if existing_questions:
                return ApiResponse(
                    code=400,
                    status="error",
                    message=f"以下问题ID已存在: {', '.join(existing_questions)}"
                )

            # 3. 批量生成向量 - 关键优化点！
            logger.info(f"开始批量向量化 {len(request.question_info_list)} 个问题")
            question_texts = [q.question for q in request.question_info_list]
            question_vectors = self.embedding_service.get_embeddings_batch(question_texts)
            logger.info(f"批量向量化完成，获得 {len(question_vectors)} 个向量")

            # 4. 批量存储
            success_count = 0
            failed_items = []
            current_time = datetime.now().isoformat()

            for i, question_info in enumerate(request.question_info_list):
                try:
                    # 构建存储数据
                    store_data = {
                        "question_id": question_info.question_id,
                        "question": question_info.question,
                        "standard_reply": question_info.standard_reply,
                        "related_links": question_info.related_links or [],
                        "category": question_info.category,
                        "query_vector": question_vectors[i],  # 使用批量获取的向量
                        "created_at": current_time,
                        "updated_at": current_time
                    }

                    # 存储到Redis
                    success = await curd.store_hotspot_question(
                        question_id=question_info.question_id,
                        data=store_data,
                        group_id=request.group_id
                    )

                    if success:
                        success_count += 1
                    else:
                        failed_items.append({
                            "question_id": question_info.question_id,
                            "reason": "存储失败"
                        })

                except Exception as e:
                    failed_items.append({
                        "question_id": question_info.question_id,
                        "reason": str(e)
                    })

            logger.info(f"批量添加完成: 成功{success_count}个, 失败{len(failed_items)}个")

            return ApiResponse(
                code=200,
                status="success",
                message=f"批量添加完成: 成功{success_count}个, 失败{len(failed_items)}个",
                data={
                    "success_count": success_count,
                    "failed_count": len(failed_items),
                    "failed_items": failed_items,
                    "total_processed": len(request.question_info_list)
                }
            )

        except Exception as e:
            logger.error(f"批量添加问题失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"批量添加失败: {str(e)}"
            )

    async def update_question(self, request: UpdateQuestionRequest) -> ApiResponse:
        """更新热点问题"""
        try:
            # 1. 获取现有数据
            existing_data = await curd.get_hotspot_question("default", request.question_id)
            if not existing_data:
                return ApiResponse(
                    code=404,
                    status="error",
                    message=f"问题ID {request.question_id} 不存在"
                )

            # 2. 更新字段
            updated_data = existing_data.copy()

            if request.question is not None:
                updated_data["question"] = request.question
                # 重新生成向量
                updated_data["query_vector"] = await self._get_text_embedding(request.question)

            if request.standard_reply is not None:
                updated_data["standard_reply"] = request.standard_reply

            if request.related_links is not None:
                updated_data["related_links"] = request.related_links

            if request.category is not None:
                updated_data["category"] = request.category

            updated_data["updated_at"] = "2025-01-01"  # DO: 使用实际时间

            # 3. 存储更新后的数据
            success = await curd.store_hotspot_question(
                question_id=request.question_id,
                data=updated_data,
                group_id="default"
            )

            if success:
                logger.info(f"成功更新问题: {request.question_id}")
                return ApiResponse(
                    code=200,
                    status="success",
                    message="问题更新成功",
                    data={"question_id": request.question_id}
                )
            else:
                return ApiResponse(
                    code=500,
                    status="error",
                    message="问题更新失败"
                )

        except Exception as e:
            logger.error(f"更新问题失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"更新问题失败: {str(e)}"
            )

    async def query_questions_batch(self, queries: List[str], group_id: str, limit: int = 3) -> ApiResponse:
        """批量查询热点问题 - 使用batch embedding提高效率"""
        try:
            if not queries:
                return ApiResponse(
                    code=400,
                    status="error",
                    message="查询列表不能为空"
                )

            logger.info(f"开始批量查询: {len(queries)} 个问题")

            # 1. 批量生成查询向量
            query_vectors = self.embedding_service.get_embeddings_batch(queries)
            logger.info(f"批量向量化完成，获得 {len(query_vectors)} 个查询向量")

            # 2. 为每个查询执行向量搜索
            all_results = []
            min_similarity = 0.5  # 最低相似度阈值

            for i, (query, query_vector) in enumerate(zip(queries, query_vectors)):
                try:
                    # 执行向量搜索
                    search_results = await curd.vector_search_questions(
                        group_id=group_id,
                        query_vector=query_vector,
                        limit=limit
                    )

                    # 过滤低相似度结果
                    filtered_results = [
                        result for result in search_results
                        if result.get('similarity_score', 0) >= min_similarity
                    ]

                    all_results.append({
                        "query": query,
                        "query_index": i,
                        "results": filtered_results,
                        "total": len(filtered_results),
                        "original_count": len(search_results)
                    })

                except Exception as e:
                    logger.error(f"查询 '{query}' 失败: {str(e)}")
                    all_results.append({
                        "query": query,
                        "query_index": i,
                        "results": [],
                        "total": 0,
                        "error": str(e)
                    })

            logger.info(f"批量查询完成: {len(queries)} 个查询已处理")

            return ApiResponse(
                code=200,
                status="success",
                message="批量查询成功",
                data={
                    "queries": all_results,
                    "total_queries": len(queries),
                    "search_params": {
                        "min_similarity": min_similarity,
                        "limit_per_query": limit
                    }
                }
            )

        except Exception as e:
            logger.error(f"批量查询失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"批量查询失败: {str(e)}"
            )

    async def query_questions(self, request: QueryRequest) -> ApiResponse:
        """查询热点问题 - 使用向量相似度搜索"""
        try:
            # 1. 将查询文本转换为向量
            logger.info(f"开始处理查询: {request.query}")
            query_vector = await self._get_text_embedding(request.query)

            # 2. 执行向量搜索
            search_results = await curd.vector_search_questions(
                group_id=request.group_id,
                query_vector=query_vector,
                limit=request.limit or 3
            )

            # 3. 过滤低相似度结果（可选）
            filtered_results = []
            min_similarity = 0.5  # 最低相似度阈值

            for result in search_results:
                similarity = result.get('similarity_score', 0)
                if similarity >= min_similarity:
                    filtered_results.append(result)
                else:
                    logger.debug(f"过滤低相似度结果: {result.get('question', 'Unknown')} (相似度: {similarity})")

            logger.info(f"查询完成: {request.query} | 原始结果: {len(search_results)}个 | 过滤后: {len(filtered_results)}个")

            return ApiResponse(
                code=200,
                status="success",
                message="查询成功",
                data={
                    "query": request.query,
                    "results": filtered_results,
                    "total": len(filtered_results),
                    "search_params": {
                        "min_similarity": min_similarity,
                        "original_count": len(search_results)
                    }
                }
            )

        except Exception as e:
            logger.error(f"查询问题失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"查询失败: {str(e)}"
            )

    async def get_question_by_id(self, group_id: str, question_id: str) -> ApiResponse:
        """根据ID获取问题详情"""
        try:
            question_data = await curd.get_hotspot_question(group_id, question_id)

            if question_data:
                return ApiResponse(
                    code=200,
                    status="success",
                    message="获取成功",
                    data=question_data
                )
            else:
                return ApiResponse(
                    code=404,
                    status="error",
                    message=f"问题ID {question_id} 不存在"
                )

        except Exception as e:
            logger.error(f"获取问题详情失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"获取失败: {str(e)}"
            )

    async def list_questions(self, group_id: str, limit: int = 50) -> ApiResponse:
        """获取问题列表"""
        try:
            questions = await curd.list_all_questions(group_id=group_id, limit=limit)

            return ApiResponse(
                code=200,
                status="success",
                message="获取列表成功",
                data={
                    "questions": questions,
                    "total": len(questions)
                }
            )

        except Exception as e:
            logger.error(f"获取问题列表失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"获取列表失败: {str(e)}"
            )

    async def delete_question(self, group_id: str, question_id: str) -> ApiResponse:
        """删除问题"""
        try:
            success = await curd.delete_hotspot_question(group_id, question_id)

            if success:
                logger.info(f"成功删除问题: {question_id}")
                return ApiResponse(
                    code=200,
                    status="success",
                    message="删除成功"
                )
            else:
                return ApiResponse(
                    code=404,
                    status="error",
                    message=f"问题ID {question_id} 不存在或删除失败"
                )

        except Exception as e:
            logger.error(f"删除问题失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"删除失败: {str(e)}"
            )

    async def get_stats(self, group_id: str) -> ApiResponse:
        """获取统计信息"""
        try:
            stats = await curd.get_stats(group_id)

            return ApiResponse(
                code=200,
                status="success",
                message="获取统计信息成功",
                data=stats
            )

        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            return ApiResponse(
                code=500,
                status="error",
                message=f"获取统计信息失败: {str(e)}"
            )


# 创建处理器实例
hotspot_handler = HotspotHandler()