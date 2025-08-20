"""
Redis Stack 数据库相关的数据模型定义
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class HotspotQuestionDocument(BaseModel):
    """热点问题在Redis中的存储格式"""
    question_id: str = Field(..., description="问题唯一标识")
    question: str = Field(..., description="问题内容")
    standard_reply: str = Field(..., description="标准回复")
    related_links: List[str] = Field(default_factory=list, description="相关链接")
    category: str = Field("通用", description="问题分类")
    query_vector: List[float] = Field(..., description="问题的向量表示")
    created_at: str = Field(..., description="创建时间")
    updated_at: str = Field(..., description="更新时间")
    hit_count: int = Field(default=0, description="命中次数")


class SearchResult(BaseModel):
    """搜索结果模型"""
    key: str = Field(..., description="Redis中的key")
    question_id: str = Field(..., description="问题ID")
    question: str = Field(..., description="问题内容")
    standard_reply: str = Field(..., description="标准回复")
    category: str = Field(..., description="问题分类")
    similarity_score: Optional[float] = Field(None, description="相似度评分")


class IndexInfo(BaseModel):
    """索引信息模型"""
    index_name: str = Field(..., description="索引名称")
    total_docs: int = Field(..., description="文档总数")
    vector_dim: int = Field(..., description="向量维度")
    distance_metric: str = Field(..., description="距离度量方式")
    status: str = Field(..., description="索引状态")


class GroupStats(BaseModel):
    """分组统计信息"""
    group_id: str = Field(..., description="分组ID")
    total_questions: int = Field(..., description="问题总数")
    categories: Dict[str, int] = Field(..., description="各分类的问题数量")
    index_status: str = Field(..., description="索引状态")
    last_updated: Optional[str] = Field(None, description="最后更新时间")