"""
热点问题路由下的对外API接口定义
"""
from fastapi import APIRouter, Body, Query, Path
from typing import Optional

from src.utils.logger import logger
from src.router.hotspot.models import (
    AddQuestionRequest, AddQuestionBatchRequest, UpdateQuestionRequest,
    QueryRequest, BatchQueryRequest, ApiResponse
)
from src.router.hotspot.handler import hotspot_handler


router = APIRouter(prefix="/hotspot")


@router.post("/questions", response_model=ApiResponse, summary="添加单个热点问题")
async def add_question(request: AddQuestionRequest):
    """
    添加单个热点问题

    - **question_info**: 问题详细信息
    - **group_id**: 问题所属分组ID（通常对应公司ID）
    """
    logger.info(f"收到添加问题请求: {request.question_info.question_id}")
    return await hotspot_handler.add_question(request)


@router.post("/questions/batch", response_model=ApiResponse, summary="批量添加热点问题")
async def add_questions_batch(request: AddQuestionBatchRequest):
    """
    批量添加热点问题

    - **question_info_list**: 问题信息列表
    - **group_id**: 问题所属分组ID
    """
    logger.info(f"收到批量添加问题请求: {len(request.question_info_list)}个问题")
    return await hotspot_handler.add_questions_batch(request)


@router.post("/questions/{question_id}/update", response_model=ApiResponse, summary="更新热点问题")
async def update_question(
    question_id: str = Path(..., description="要更新的问题ID"),
    question: Optional[str] = Body(None, description="更新的问题内容"),
    standard_reply: Optional[str] = Body(None, description="更新的标准回复"),
    related_links: Optional[list] = Body(None, description="更新的相关链接"),
    category: Optional[str] = Body(None, description="更新的问题分类")
):
    """
    更新热点问题

    可以部分更新问题的字段，传入的字段将被更新，未传入的字段保持不变
    """
    # 构建更新请求
    update_request = UpdateQuestionRequest(
        question_id=question_id,
        question=question,
        standard_reply=standard_reply,
        related_links=related_links,
        category=category
    )

    logger.info(f"收到更新问题请求: {question_id}")
    return await hotspot_handler.update_question(update_request)


@router.post("/query/batch", response_model=ApiResponse, summary="批量查询热点问题")
async def query_questions_batch(
    queries: list[str] = Body(..., description="查询文本列表"),
    group_id: str = Body(..., description="分组ID"),
    limit: Optional[int] = Body(3, description="每个查询返回的结果数量限制")
):
    """
    批量查询热点问题，使用batch embedding提高效率

    - **queries**: 查询文本列表
    - **group_id**: 查询范围所属分组ID
    - **limit**: 每个查询返回的结果数量限制（默认3个）
    """
    logger.info(f"收到批量查询请求: {len(queries)} 个查询")
    return await hotspot_handler.query_questions_batch(queries, group_id, limit)


@router.post("/query", response_model=ApiResponse, summary="查询热点问题")
async def query_questions(request: QueryRequest):
    """
    基于自然语言查询热点问题

    - **query**: 查询文本
    - **limit**: 返回结果数量限制（默认3个）
    - **group_id**: 查询范围所属分组ID
    """
    logger.info(f"收到查询请求: {request.query}")
    return await hotspot_handler.query_questions(request)


@router.get("/questions/{question_id}", response_model=ApiResponse, summary="获取问题详情")
async def get_question_by_id(
    question_id: str = Path(..., description="问题ID"),
    group_id: str = Query(..., description="分组ID")
):
    """
    根据问题ID获取问题详细信息
    """
    logger.info(f"收到获取问题详情请求: {question_id}")
    return await hotspot_handler.get_question_by_id(group_id, question_id)


@router.get("/questions", response_model=ApiResponse, summary="获取问题列表")
async def list_questions(
    group_id: str = Query(..., description="分组ID"),
    limit: int = Query(50, description="返回数量限制", ge=1, le=1000)
):
    """
    获取指定分组的所有热点问题列表
    """
    logger.info(f"收到获取问题列表请求: group_id={group_id}, limit={limit}")
    return await hotspot_handler.list_questions(group_id, limit)


@router.post("/questions/{question_id}/delete", response_model=ApiResponse, summary="删除热点问题")
async def delete_question(
    question_id: str = Path(..., description="要删除的问题ID"),
    group_id: str = Body(..., description="分组ID")
):
    """
    删除指定的热点问题
    """
    logger.info(f"收到删除问题请求: {question_id}")
    return await hotspot_handler.delete_question(group_id, question_id)


@router.get("/stats", response_model=ApiResponse, summary="获取统计信息")
async def get_stats(
    group_id: str = Query(..., description="分组ID")
):
    """
    获取指定分组的统计信息

    包括：
    - 问题总数
    - 各分类问题数量
    - 索引状态等
    """
    logger.info(f"收到获取统计信息请求: group_id={group_id}")
    return await hotspot_handler.get_stats(group_id)


# 健康检查接口
@router.get("/health", summary="健康检查")
async def health_check():
    """
    健康检查接口
    """
    return {"status": "healthy", "service": "hotspot"}


# 创建索引接口（管理员使用）
@router.post("/admin/index/{group_id}", response_model=ApiResponse, summary="创建索引")
async def create_index(
    group_id: str = Path(..., description="要创建索引的分组ID")
):
    """
    为指定分组创建向量索引（管理员接口）
    """
    from src.dbs.redis_stack import curd

    logger.info(f"收到创建索引请求: {group_id}")

    try:
        success = await curd.create_hotspot_index(group_id)
        if success:
            return ApiResponse(
                code=200,
                status="success",
                message=f"索引 {group_id} 创建成功"
            )
        else:
            return ApiResponse(
                code=500,
                status="error",
                message=f"索引 {group_id} 创建失败"
            )
    except Exception as e:
        logger.error(f"创建索引失败: {str(e)}")
        return ApiResponse(
            code=500,
            status="error",
            message=f"创建索引失败: {str(e)}"
        )