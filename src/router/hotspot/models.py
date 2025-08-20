"""
热点问题接口的数据结构 - 添加批量处理支持
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Any


class QuestionInfo(BaseModel):
    """问题信息"""
    question_id: str = Field(..., description='要添加的问题id')
    question: str = Field(..., description='要添加的问题本身')
    standard_reply: str = Field(..., description='该问题的标准回答')
    related_links: Optional[List[str]] = Field([],description='与该问题有关的相关资料链接')
    category: str = Field("通用", description='添加问题的分类')


class AddQuestionRequest(BaseModel):
    """添加热点问题的数据结构"""
    question_info: QuestionInfo
    group_id: str = Field(..., description='该问题属于的分组，一个公司默认只有一个分组')


class AddQuestionBatchRequest(BaseModel):
    """批量添加热点问题的数据结构"""
    question_info_list: list[QuestionInfo]
    group_id: str = Field(..., description='该问题属于的分组，一个公司默认只有一个分组')


class UpdateQuestionRequest(BaseModel):
    """更新热点问题的数据结构"""
    question_id: str = Field(..., description='必填项, 要更新的问题id')
    question: Optional[str] = Field(None, description='可选项，要更新的问题本身')
    standard_reply: Optional[str] = Field(None, description='可选项，要更新的标准回复')
    related_links: Optional[List[str]] = Field(None, description='可选项，要更新的相关链接')
    category: Optional[str] = Field(None, description='可选项，要更新的问题分类')


class QueryRequest(BaseModel):
    """询问热点问题"""
    query: str
    limit: Optional[int] = 3
    group_id: str = Field(..., description='该问题属于的分组，一个公司默认只有一个分组')


class BatchQueryRequest(BaseModel):
    """批量查询热点问题"""
    queries: List[str] = Field(..., description="查询文本列表")
    limit: Optional[int] = Field(3, description="每个查询返回的结果数量限制")
    group_id: str = Field(..., description="该问题属于的分组，一个公司默认只有一个分组")


class QueryResult(BaseModel):
    """单个查询结果"""
    query: str = Field(..., description="查询文本")
    query_index: int = Field(..., description="查询在批量请求中的索引")
    results: List[dict] = Field(..., description="搜索结果列表")
    total: int = Field(..., description="结果总数")
    original_count: Optional[int] = Field(None, description="过滤前的原始结果数量")
    error: Optional[str] = Field(None, description="查询错误信息")


class BatchQueryResponse(BaseModel):
    """批量查询响应"""
    queries: List[QueryResult] = Field(..., description="所有查询的结果")
    total_queries: int = Field(..., description="总查询数量")
    search_params: dict = Field(..., description="搜索参数")


class ApiResponse(BaseModel):
    """回复数据"""
    code: int
    status: str
    message: str
    data: Optional[Any] = None